package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/jamesainslie/gomq/amqp"
	"github.com/jamesainslie/gomq/storage"
)

// sendMethodFn writes a method frame to the connection on the given channel.
type sendMethodFn func(channel uint16, method amqp.Method) error

// sendContentFn writes a content header and body to the connection.
type sendContentFn func(channel uint16, classID uint16, props *amqp.Properties, body []byte) error

// sendDeliveryFn writes a method, content header, and body in a single flush.
type sendDeliveryFn func(channel uint16, method amqp.Method, classID uint16, props *amqp.Properties, body []byte) error

// defaultBodyBufSize is the initial capacity for pooled publish body buffers.
const defaultBodyBufSize = 4096

// bodyPool reuses byte buffers for publish body accumulation to avoid
// per-publish heap allocations. Stores *[]byte to satisfy SA6002.
//
//nolint:gochecknoglobals // sync.Pool is inherently global
var bodyPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, defaultBodyBufSize)
		return &b
	},
}

// Unack tracks a delivered but unacknowledged message.
type Unack struct {
	DeliveryTag uint64
	Queue       *Queue
	SP          storage.SegmentPosition
	ConsumerTag string
}

// Channel implements the AMQP channel state machine. It processes methods
// dispatched by the connection read loop, manages consumers, accumulates
// publish content, and tracks unacknowledged deliveries.
type Channel struct {
	mu            sync.Mutex
	id            uint16
	vhost         *VHost
	consumers     map[string]*Consumer
	unacked       []Unack
	deliveryTag   uint64
	confirmMode   bool
	confirmCount  uint64
	prefetchCount uint16
	closed        bool

	// Publish accumulation state.
	pubExchange   string
	pubRoutingKey string
	pubMandatory  bool
	pubImmediate  bool
	pubProperties amqp.Properties
	pubBodySize   uint64
	pubBody       []byte
	pubBodyBufp   *[]byte // pool pointer for returning the buffer
	pubState      publishState

	sendMethod   sendMethodFn
	sendContent  sendContentFn
	sendDelivery sendDeliveryFn
}

// publishState tracks progress through the BasicPublish + Header + Body sequence.
type publishState int

const (
	pubStateIdle   publishState = iota
	pubStateHeader              // received BasicPublish, waiting for header
	pubStateBody                // received header, accumulating body
)

// NewChannel creates a channel bound to a vhost with write callbacks.
func NewChannel(id uint16, vhost *VHost, sendMethod sendMethodFn, sendContent sendContentFn, sendDelivery sendDeliveryFn) *Channel {
	return &Channel{
		id:           id,
		vhost:        vhost,
		consumers:    make(map[string]*Consumer),
		sendMethod:   sendMethod,
		sendContent:  sendContent,
		sendDelivery: sendDelivery,
	}
}

// HandleMethod dispatches an AMQP method to the appropriate handler.
func (ch *Channel) HandleMethod(method amqp.Method) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return fmt.Errorf("channel %d is closed", ch.id)
	}

	switch method := method.(type) {
	// Exchange operations.
	case *amqp.ExchangeDeclare:
		return ch.handleExchangeDeclare(method)
	case *amqp.ExchangeDelete:
		return ch.handleExchangeDelete(method)

	// Queue operations.
	case *amqp.QueueDeclare:
		return ch.handleQueueDeclare(method)
	case *amqp.QueueDelete:
		return ch.handleQueueDelete(method)
	case *amqp.QueueBind:
		return ch.handleQueueBind(method)
	case *amqp.QueueUnbind:
		return ch.handleQueueUnbind(method)
	case *amqp.QueuePurge:
		return ch.handleQueuePurge(method)

	// Basic operations.
	case *amqp.BasicPublish:
		ch.handleBasicPublish(method)
		return nil
	case *amqp.BasicConsume:
		return ch.handleBasicConsume(method)
	case *amqp.BasicCancel:
		return ch.handleBasicCancel(method)
	case *amqp.BasicGet:
		return ch.handleBasicGet(method)
	case *amqp.BasicAck:
		return ch.handleBasicAck(method)
	case *amqp.BasicNack:
		return ch.handleBasicNack(method)
	case *amqp.BasicReject:
		return ch.handleBasicReject(method)
	case *amqp.BasicQos:
		return ch.handleBasicQos(method)

	// Confirm.
	case *amqp.ConfirmSelect:
		return ch.handleConfirmSelect(method)

	// Tx.
	case *amqp.TxSelect:
		return ch.send(&amqp.TxSelectOk{})
	case *amqp.TxCommit:
		return ch.send(&amqp.TxCommitOk{})
	case *amqp.TxRollback:
		return ch.send(&amqp.TxRollbackOk{})

	// Channel flow.
	case *amqp.ChannelFlow:
		return ch.send(&amqp.ChannelFlowOk{Active: method.Active})

	default:
		return fmt.Errorf("channel %d: unhandled method %s", ch.id, method.MethodName())
	}
}

// HandleHeader processes a content header frame during publish accumulation.
func (ch *Channel) HandleHeader(hf *amqp.HeaderFrame) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.pubState != pubStateHeader {
		return fmt.Errorf("channel %d: unexpected header frame", ch.id)
	}

	ch.pubProperties = hf.Properties
	ch.pubBodySize = hf.BodySize

	poolVal := bodyPool.Get()
	bufp, ok := poolVal.(*[]byte)
	if !ok {
		b := make([]byte, 0, defaultBodyBufSize)
		bufp = &b
	}
	buf := *bufp
	if uint64(cap(buf)) < hf.BodySize {
		buf = make([]byte, 0, hf.BodySize)
	}
	ch.pubBody = buf[:0]
	ch.pubBodyBufp = bufp

	if hf.BodySize == 0 {
		return ch.finishPublish()
	}

	ch.pubState = pubStateBody
	return nil
}

// HandleBody processes a content body frame during publish accumulation.
func (ch *Channel) HandleBody(bf *amqp.BodyFrame) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.pubState != pubStateBody {
		return fmt.Errorf("channel %d: unexpected body frame", ch.id)
	}

	ch.pubBody = append(ch.pubBody, bf.Body...)

	if uint64(len(ch.pubBody)) >= ch.pubBodySize {
		return ch.finishPublish()
	}

	return nil
}

// Close stops all consumers and marks the channel as closed.
func (ch *Channel) Close() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return
	}
	ch.closed = true

	for _, c := range ch.consumers {
		c.Stop()
	}
	clear(ch.consumers)
}

// --- Exchange handlers ---

func (ch *Channel) handleExchangeDeclare(m *amqp.ExchangeDeclare) error {
	_, err := ch.vhost.DeclareExchange(m.Exchange, m.Type, m.Durable, m.AutoDelete, nil)
	if err != nil {
		return fmt.Errorf("exchange.declare: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.ExchangeDeclareOk{})
	}
	return nil
}

func (ch *Channel) handleExchangeDelete(m *amqp.ExchangeDelete) error {
	if err := ch.vhost.DeleteExchange(m.Exchange, m.IfUnused); err != nil {
		return fmt.Errorf("exchange.delete: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.ExchangeDeleteOk{})
	}
	return nil
}

// --- Queue handlers ---

func (ch *Channel) handleQueueDeclare(m *amqp.QueueDeclare) error {
	queue, err := ch.vhost.DeclareQueue(m.Queue, m.Durable, m.Exclusive, m.AutoDelete, tableToMap(m.Arguments))
	if err != nil {
		return fmt.Errorf("queue.declare: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.QueueDeclareOk{
			Queue:         queue.Name(),
			MessageCount:  queue.Len(),
			ConsumerCount: uint32(queue.ConsumerCount()), //nolint:gosec // consumer count is always small
		})
	}
	return nil
}

func (ch *Channel) handleQueueDelete(m *amqp.QueueDelete) error {
	count, err := ch.vhost.DeleteQueue(m.Queue, m.IfUnused, m.IfEmpty)
	if err != nil {
		return fmt.Errorf("queue.delete: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.QueueDeleteOk{MessageCount: count})
	}
	return nil
}

func (ch *Channel) handleQueueBind(m *amqp.QueueBind) error {
	if err := ch.vhost.BindQueue(m.Queue, m.Exchange, m.RoutingKey, tableToMap(m.Arguments)); err != nil {
		return fmt.Errorf("queue.bind: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.QueueBindOk{})
	}
	return nil
}

func (ch *Channel) handleQueueUnbind(m *amqp.QueueUnbind) error {
	if err := ch.vhost.UnbindQueue(m.Queue, m.Exchange, m.RoutingKey, tableToMap(m.Arguments)); err != nil {
		return fmt.Errorf("queue.unbind: %w", err)
	}
	return ch.send(&amqp.QueueUnbindOk{})
}

func (ch *Channel) handleQueuePurge(m *amqp.QueuePurge) error {
	count, err := ch.vhost.PurgeQueue(m.Queue)
	if err != nil {
		return fmt.Errorf("queue.purge: %w", err)
	}
	if !m.NoWait {
		return ch.send(&amqp.QueuePurgeOk{MessageCount: count})
	}
	return nil
}

// --- Basic handlers ---

func (ch *Channel) handleBasicPublish(pub *amqp.BasicPublish) {
	ch.pubExchange = pub.Exchange
	ch.pubRoutingKey = pub.RoutingKey
	ch.pubMandatory = pub.Mandatory
	ch.pubImmediate = pub.Immediate
	ch.pubState = pubStateHeader
}

func (ch *Channel) handleBasicConsume(consume *amqp.BasicConsume) error {
	queue, ok := ch.vhost.GetQueue(consume.Queue)
	if !ok {
		return fmt.Errorf("queue %q: %w", consume.Queue, ErrQueueNotFound)
	}

	tag := consume.ConsumerTag
	if tag == "" {
		tag = fmt.Sprintf("ctag-%d-%d", ch.id, len(ch.consumers)+1)
	}

	prefetch := ch.prefetchCount
	noAck := consume.NoAck

	// Parse consumer priority from arguments (x-priority, default 0).
	var priority int
	if consume.Arguments != nil {
		if v, ok := consume.Arguments["x-priority"]; ok {
			priority = int(toInt64(v))
		}
	}

	chID := ch.id
	consumer := NewConsumer(tag, queue, noAck, consume.Exclusive, prefetch,
		func(env *storage.Envelope, deliveryTag uint64) error {
			// Send BasicDeliver + content header + body in a single flush.
			// The envelope's Body may alias an mmap region (zero-copy path),
			// so we write to the socket here while the region is still valid.
			props := storagePropsToAMQP(env.Message.Properties)
			if sendErr := ch.sendDelivery(chID, &amqp.BasicDeliver{
				ConsumerTag: tag,
				DeliveryTag: deliveryTag,
				Redelivered: env.Redelivered,
				Exchange:    env.Message.ExchangeName,
				RoutingKey:  env.Message.RoutingKey,
			}, amqp.ClassBasic, &props, env.Message.Body); sendErr != nil {
				return fmt.Errorf("send delivery: %w", sendErr)
			}

			if !noAck {
				// Copy only metadata needed for ack processing — not the body.
				ch.mu.Lock()
				ch.unacked = append(ch.unacked, Unack{
					DeliveryTag: deliveryTag,
					Queue:       queue,
					SP:          env.SegmentPosition,
					ConsumerTag: tag,
				})
				ch.mu.Unlock()
			}
			return nil
		},
	)

	consumer.SetPriority(priority)

	if err := queue.AddConsumer(consumer.stub()); err != nil {
		return fmt.Errorf("basic.consume: %w", err)
	}

	ch.consumers[tag] = consumer
	consumer.Start()

	if !consume.NoWait {
		return ch.send(&amqp.BasicConsumeOk{ConsumerTag: tag})
	}
	return nil
}

func (ch *Channel) handleBasicCancel(cancel *amqp.BasicCancel) error {
	consumer, ok := ch.consumers[cancel.ConsumerTag]
	if !ok {
		return fmt.Errorf("consumer %q not found", cancel.ConsumerTag)
	}

	consumer.Stop()
	consumer.queue.RemoveConsumer(cancel.ConsumerTag)
	delete(ch.consumers, cancel.ConsumerTag)

	if !cancel.NoWait {
		return ch.send(&amqp.BasicCancelOk{ConsumerTag: cancel.ConsumerTag})
	}
	return nil
}

func (ch *Channel) handleBasicGet(get *amqp.BasicGet) error {
	queue, ok := ch.vhost.GetQueue(get.Queue)
	if !ok {
		return fmt.Errorf("queue %q: %w", get.Queue, ErrQueueNotFound)
	}

	// Drain the async inbox so recently published messages are visible
	// for the synchronous basic.get polling operation.
	queue.Drain()

	env, got := queue.Get(get.NoAck)
	if !got {
		return ch.send(&amqp.BasicGetEmpty{})
	}

	ch.deliveryTag++
	tag := ch.deliveryTag

	if !get.NoAck {
		ch.unacked = append(ch.unacked, Unack{
			DeliveryTag: tag,
			Queue:       queue,
			SP:          env.SegmentPosition,
		})
	}

	if err := ch.send(&amqp.BasicGetOk{
		DeliveryTag:  tag,
		Redelivered:  env.Redelivered,
		Exchange:     env.Message.ExchangeName,
		RoutingKey:   env.Message.RoutingKey,
		MessageCount: queue.Len(),
	}); err != nil {
		return err
	}

	props := storagePropsToAMQP(env.Message.Properties)
	return ch.sendContent(ch.id, amqp.ClassBasic, &props, env.Message.Body)
}

func (ch *Channel) handleBasicAck(m *amqp.BasicAck) error {
	if m.Multiple {
		return ch.ackMultiple(m.DeliveryTag)
	}
	return ch.ackSingle(m.DeliveryTag)
}

func (ch *Channel) handleBasicNack(m *amqp.BasicNack) error {
	if m.Multiple {
		return ch.rejectMultiple(m.DeliveryTag, m.Requeue)
	}
	return ch.rejectSingle(m.DeliveryTag, m.Requeue)
}

func (ch *Channel) handleBasicReject(m *amqp.BasicReject) error {
	return ch.rejectSingle(m.DeliveryTag, m.Requeue)
}

func (ch *Channel) handleBasicQos(m *amqp.BasicQos) error {
	ch.prefetchCount = m.PrefetchCount
	return ch.send(&amqp.BasicQosOk{})
}

// --- Confirm handler ---

func (ch *Channel) handleConfirmSelect(_ *amqp.ConfirmSelect) error {
	ch.confirmMode = true
	ch.confirmCount = 0
	return ch.send(&amqp.ConfirmSelectOk{})
}

// --- Publish completion ---

func (ch *Channel) finishPublish() error {
	ch.pubState = pubStateIdle

	msg := &storage.Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: ch.pubExchange,
		RoutingKey:   ch.pubRoutingKey,
		Properties:   amqpPropsToStorage(ch.pubProperties),
		BodySize:     uint64(len(ch.pubBody)),
		Body:         ch.pubBody,
	}

	// Use synchronous persistence when confirms are enabled — the BasicAck
	// must not be sent until the message is on disk.
	syncPersist := ch.confirmMode

	var routed bool
	var err error
	if ch.pubMandatory {
		routed, err = ch.vhost.PublishMandatory(ch.pubExchange, ch.pubRoutingKey, syncPersist, msg)
	} else {
		err = ch.vhost.Publish(ch.pubExchange, ch.pubRoutingKey, syncPersist, msg)
		routed = true // non-mandatory: we don't care about route status
	}

	// Capture body and props before returning the buffer to pool.
	returnBody := ch.pubBody
	returnProps := ch.pubProperties
	returnExchange := ch.pubExchange
	returnRoutingKey := ch.pubRoutingKey

	// Return body buffer to pool after publish is complete.
	returnNeeded := ch.pubMandatory && !routed && err == nil
	if ch.pubBodyBufp != nil && !returnNeeded {
		*ch.pubBodyBufp = ch.pubBody[:0]
		bodyPool.Put(ch.pubBodyBufp)
		ch.pubBodyBufp = nil
		ch.pubBody = nil
	}

	// Send Basic.Return for mandatory messages that could not be routed.
	if returnNeeded {
		ch.mu.Unlock()
		returnErr := ch.sendMethod(ch.id, &amqp.BasicReturn{
			ReplyCode:  amqp.NoRoute,
			ReplyText:  "NO_ROUTE",
			Exchange:   returnExchange,
			RoutingKey: returnRoutingKey,
		})
		if returnErr == nil {
			returnErr = ch.sendContent(ch.id, amqp.ClassBasic, &returnProps, returnBody)
		}
		ch.mu.Lock()

		// Now return the body buffer.
		if ch.pubBodyBufp != nil {
			*ch.pubBodyBufp = ch.pubBody[:0]
			bodyPool.Put(ch.pubBodyBufp)
			ch.pubBodyBufp = nil
			ch.pubBody = nil
		}

		if returnErr != nil {
			return fmt.Errorf("send basic.return: %w", returnErr)
		}
	}

	if ch.confirmMode {
		ch.confirmCount++
		tag := ch.confirmCount
		ch.mu.Unlock()
		sendErr := ch.sendMethod(ch.id, &amqp.BasicAck{DeliveryTag: tag})
		ch.mu.Lock()
		if sendErr != nil {
			return fmt.Errorf("send confirm ack: %w", sendErr)
		}
	}

	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

// --- Ack/Nack helpers ---

func (ch *Channel) ackSingle(tag uint64) error {
	for idx, entry := range ch.unacked {
		if entry.DeliveryTag == tag {
			if err := entry.Queue.Ack(entry.SP); err != nil {
				return fmt.Errorf("ack delivery %d: %w", tag, err)
			}
			// Notify the consumer about freed capacity.
			if consumer, ok := ch.consumers[entry.ConsumerTag]; ok {
				consumer.Ack()
			}
			ch.unacked = append(ch.unacked[:idx], ch.unacked[idx+1:]...)
			return nil
		}
	}
	return fmt.Errorf("delivery tag %d not found in unacked", tag)
}

func (ch *Channel) ackMultiple(upToTag uint64) error {
	remaining := ch.unacked[:0]
	for _, entry := range ch.unacked {
		if entry.DeliveryTag <= upToTag {
			if err := entry.Queue.Ack(entry.SP); err != nil {
				return fmt.Errorf("ack delivery %d: %w", entry.DeliveryTag, err)
			}
			if consumer, ok := ch.consumers[entry.ConsumerTag]; ok {
				consumer.Ack()
			}
		} else {
			remaining = append(remaining, entry)
		}
	}
	ch.unacked = remaining
	return nil
}

func (ch *Channel) rejectSingle(tag uint64, requeue bool) error {
	for idx, entry := range ch.unacked {
		if entry.DeliveryTag == tag {
			if err := entry.Queue.Reject(entry.SP, requeue); err != nil {
				return fmt.Errorf("reject delivery %d: %w", tag, err)
			}
			if consumer, ok := ch.consumers[entry.ConsumerTag]; ok {
				consumer.Ack()
			}
			ch.unacked = append(ch.unacked[:idx], ch.unacked[idx+1:]...)
			return nil
		}
	}
	return fmt.Errorf("delivery tag %d not found in unacked", tag)
}

func (ch *Channel) rejectMultiple(upToTag uint64, requeue bool) error {
	remaining := ch.unacked[:0]
	for _, entry := range ch.unacked {
		if entry.DeliveryTag <= upToTag {
			if err := entry.Queue.Reject(entry.SP, requeue); err != nil {
				return fmt.Errorf("reject delivery %d: %w", entry.DeliveryTag, err)
			}
			if consumer, ok := ch.consumers[entry.ConsumerTag]; ok {
				consumer.Ack()
			}
		} else {
			remaining = append(remaining, entry)
		}
	}
	ch.unacked = remaining
	return nil
}

// --- Write helpers ---

func (ch *Channel) send(method amqp.Method) error {
	return ch.sendMethod(ch.id, method)
}

// --- Property conversion ---

// tableToMap converts an amqp.Table to a generic map for broker internals.
func tableToMap(t amqp.Table) map[string]interface{} {
	if t == nil {
		return nil
	}
	m := make(map[string]interface{}, len(t))
	for k, v := range t {
		m[k] = v
	}
	return m
}

// storagePropsToAMQP converts storage properties to AMQP properties for delivery.
func storagePropsToAMQP(sp storage.Properties) amqp.Properties {
	return amqp.Properties{
		Headers:    headerMapToTable(sp.Headers),
		Expiration: sp.Expiration,
	}
}

// amqpPropsToStorage converts AMQP properties to storage properties for persistence.
func amqpPropsToStorage(ap amqp.Properties) storage.Properties {
	return storage.Properties{
		Flags:      ap.Flags(),
		Headers:    tableToHeaderMap(ap.Headers),
		Expiration: ap.Expiration,
	}
}

// headerMapToTable converts a map[string]any to an amqp.Table, recursively
// converting nested maps and slices so that the AMQP table encoder can
// serialize them correctly.
func headerMapToTable(m map[string]any) amqp.Table {
	if m == nil {
		return nil
	}
	t := make(amqp.Table, len(m))
	for k, v := range m {
		t[k] = convertHeaderValue(v)
	}
	return t
}

// convertHeaderValue converts nested map[string]any to amqp.Table and
// []any elements recursively so they match the types expected by the
// AMQP field table marshaler.
func convertHeaderValue(value any) any {
	switch val := value.(type) {
	case map[string]any:
		return headerMapToTable(val)
	case []any:
		out := make([]any, len(val))
		for i, elem := range val {
			out[i] = convertHeaderValue(elem)
		}
		return out
	default:
		return value
	}
}

// tableToHeaderMap converts an amqp.Table to a map[string]any.
func tableToHeaderMap(t amqp.Table) map[string]any {
	if t == nil {
		return nil
	}
	m := make(map[string]any, len(t))
	for k, v := range t {
		m[k] = v
	}
	return m
}
