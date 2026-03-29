package amqp

import (
	"fmt"
	"io"
)

// --- Exchange methods ---

// ExchangeDeclare declares an exchange.
type ExchangeDeclare struct {
	Reserved1  uint16
	Exchange   string // shortstr
	Type       string // shortstr
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  Table
}

func (*ExchangeDeclare) MethodID() uint32   { return MethodExchangeDeclare }
func (*ExchangeDeclare) MethodName() string { return "exchange.declare" }

func (ed *ExchangeDeclare) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if ed.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("exchange.declare read reserved-1: %w", err)
	}
	if ed.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("exchange.declare read exchange: %w", err)
	}
	if ed.Type, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("exchange.declare read type: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("exchange.declare read bits: %w", err)
	}
	ed.Passive = bits&1 != 0
	ed.Durable = bits&bit1 != 0
	ed.AutoDelete = bits&bit2 != 0
	ed.Internal = bits&bit3 != 0
	ed.NoWait = bits&bit4 != 0
	if ed.Arguments, err = rd.readTable(); err != nil {
		return fmt.Errorf("exchange.declare read arguments: %w", err)
	}
	return nil
}

func (ed *ExchangeDeclare) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(ed.Reserved1); err != nil {
		return fmt.Errorf("exchange.declare write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(ed.Exchange); err != nil {
		return fmt.Errorf("exchange.declare write exchange: %w", err)
	}
	if err := wt.writeShortstr(ed.Type); err != nil {
		return fmt.Errorf("exchange.declare write type: %w", err)
	}
	var bits uint8
	if ed.Passive {
		bits |= 1
	}
	if ed.Durable {
		bits |= bit1
	}
	if ed.AutoDelete {
		bits |= bit2
	}
	if ed.Internal {
		bits |= bit3
	}
	if ed.NoWait {
		bits |= bit4
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("exchange.declare write bits: %w", err)
	}
	if err := wt.writeTable(ed.Arguments); err != nil {
		return fmt.Errorf("exchange.declare write arguments: %w", err)
	}
	return nil
}

// ExchangeDeclareOk confirms the exchange declaration.
type ExchangeDeclareOk struct{}

func (*ExchangeDeclareOk) MethodID() uint32        { return MethodExchangeDeclareOk }
func (*ExchangeDeclareOk) MethodName() string      { return "exchange.declare-ok" }
func (*ExchangeDeclareOk) Read(_ io.Reader) error  { return nil }
func (*ExchangeDeclareOk) Write(_ io.Writer) error { return nil }

// ExchangeDelete deletes an exchange.
type ExchangeDelete struct {
	Reserved1 uint16
	Exchange  string // shortstr
	IfUnused  bool
	NoWait    bool
}

func (*ExchangeDelete) MethodID() uint32   { return MethodExchangeDelete }
func (*ExchangeDelete) MethodName() string { return "exchange.delete" }

func (ed *ExchangeDelete) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if ed.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("exchange.delete read reserved-1: %w", err)
	}
	if ed.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("exchange.delete read exchange: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("exchange.delete read bits: %w", err)
	}
	ed.IfUnused = bits&1 != 0
	ed.NoWait = bits&bit1 != 0
	return nil
}

func (ed *ExchangeDelete) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(ed.Reserved1); err != nil {
		return fmt.Errorf("exchange.delete write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(ed.Exchange); err != nil {
		return fmt.Errorf("exchange.delete write exchange: %w", err)
	}
	var bits uint8
	if ed.IfUnused {
		bits |= 1
	}
	if ed.NoWait {
		bits |= bit1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("exchange.delete write bits: %w", err)
	}
	return nil
}

// ExchangeDeleteOk confirms the exchange deletion.
type ExchangeDeleteOk struct{}

func (*ExchangeDeleteOk) MethodID() uint32        { return MethodExchangeDeleteOk }
func (*ExchangeDeleteOk) MethodName() string      { return "exchange.delete-ok" }
func (*ExchangeDeleteOk) Read(_ io.Reader) error  { return nil }
func (*ExchangeDeleteOk) Write(_ io.Writer) error { return nil }

// --- Queue methods ---

// QueueDeclare declares a queue.
type QueueDeclare struct {
	Reserved1  uint16
	Queue      string // shortstr
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  Table
}

func (*QueueDeclare) MethodID() uint32   { return MethodQueueDeclare }
func (*QueueDeclare) MethodName() string { return "queue.declare" }

func (qd *QueueDeclare) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qd.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("queue.declare read reserved-1: %w", err)
	}
	if qd.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.declare read queue: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("queue.declare read bits: %w", err)
	}
	qd.Passive = bits&1 != 0
	qd.Durable = bits&bit1 != 0
	qd.Exclusive = bits&bit2 != 0
	qd.AutoDelete = bits&bit3 != 0
	qd.NoWait = bits&bit4 != 0
	if qd.Arguments, err = rd.readTable(); err != nil {
		return fmt.Errorf("queue.declare read arguments: %w", err)
	}
	return nil
}

func (qd *QueueDeclare) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(qd.Reserved1); err != nil {
		return fmt.Errorf("queue.declare write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(qd.Queue); err != nil {
		return fmt.Errorf("queue.declare write queue: %w", err)
	}
	var bits uint8
	if qd.Passive {
		bits |= 1
	}
	if qd.Durable {
		bits |= bit1
	}
	if qd.Exclusive {
		bits |= bit2
	}
	if qd.AutoDelete {
		bits |= bit3
	}
	if qd.NoWait {
		bits |= bit4
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("queue.declare write bits: %w", err)
	}
	if err := wt.writeTable(qd.Arguments); err != nil {
		return fmt.Errorf("queue.declare write arguments: %w", err)
	}
	return nil
}

// QueueDeclareOk confirms the queue declaration.
type QueueDeclareOk struct {
	Queue         string // shortstr
	MessageCount  uint32
	ConsumerCount uint32
}

func (*QueueDeclareOk) MethodID() uint32   { return MethodQueueDeclareOk }
func (*QueueDeclareOk) MethodName() string { return "queue.declare-ok" }

func (qd *QueueDeclareOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qd.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.declare-ok read queue: %w", err)
	}
	if qd.MessageCount, err = rd.readUint32(); err != nil {
		return fmt.Errorf("queue.declare-ok read message-count: %w", err)
	}
	if qd.ConsumerCount, err = rd.readUint32(); err != nil {
		return fmt.Errorf("queue.declare-ok read consumer-count: %w", err)
	}
	return nil
}

func (qd *QueueDeclareOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeShortstr(qd.Queue); err != nil {
		return fmt.Errorf("queue.declare-ok write queue: %w", err)
	}
	if err := wt.writeUint32(qd.MessageCount); err != nil {
		return fmt.Errorf("queue.declare-ok write message-count: %w", err)
	}
	if err := wt.writeUint32(qd.ConsumerCount); err != nil {
		return fmt.Errorf("queue.declare-ok write consumer-count: %w", err)
	}
	return nil
}

// QueueBind binds a queue to an exchange.
type QueueBind struct {
	Reserved1  uint16
	Queue      string // shortstr
	Exchange   string // shortstr
	RoutingKey string // shortstr
	NoWait     bool
	Arguments  Table
}

func (*QueueBind) MethodID() uint32   { return MethodQueueBind }
func (*QueueBind) MethodName() string { return "queue.bind" }

func (qb *QueueBind) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qb.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("queue.bind read reserved-1: %w", err)
	}
	if qb.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.bind read queue: %w", err)
	}
	if qb.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.bind read exchange: %w", err)
	}
	if qb.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.bind read routing-key: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("queue.bind read bits: %w", err)
	}
	qb.NoWait = bits&1 != 0
	if qb.Arguments, err = rd.readTable(); err != nil {
		return fmt.Errorf("queue.bind read arguments: %w", err)
	}
	return nil
}

func (qb *QueueBind) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(qb.Reserved1); err != nil {
		return fmt.Errorf("queue.bind write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(qb.Queue); err != nil {
		return fmt.Errorf("queue.bind write queue: %w", err)
	}
	if err := wt.writeShortstr(qb.Exchange); err != nil {
		return fmt.Errorf("queue.bind write exchange: %w", err)
	}
	if err := wt.writeShortstr(qb.RoutingKey); err != nil {
		return fmt.Errorf("queue.bind write routing-key: %w", err)
	}
	var bits uint8
	if qb.NoWait {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("queue.bind write bits: %w", err)
	}
	if err := wt.writeTable(qb.Arguments); err != nil {
		return fmt.Errorf("queue.bind write arguments: %w", err)
	}
	return nil
}

// QueueBindOk confirms the queue binding.
type QueueBindOk struct{}

func (*QueueBindOk) MethodID() uint32        { return MethodQueueBindOk }
func (*QueueBindOk) MethodName() string      { return "queue.bind-ok" }
func (*QueueBindOk) Read(_ io.Reader) error  { return nil }
func (*QueueBindOk) Write(_ io.Writer) error { return nil }

// QueueUnbind removes a queue binding.
type QueueUnbind struct {
	Reserved1  uint16
	Queue      string // shortstr
	Exchange   string // shortstr
	RoutingKey string // shortstr
	Arguments  Table
}

func (*QueueUnbind) MethodID() uint32   { return MethodQueueUnbind }
func (*QueueUnbind) MethodName() string { return "queue.unbind" }

func (qu *QueueUnbind) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qu.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("queue.unbind read reserved-1: %w", err)
	}
	if qu.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.unbind read queue: %w", err)
	}
	if qu.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.unbind read exchange: %w", err)
	}
	if qu.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.unbind read routing-key: %w", err)
	}
	if qu.Arguments, err = rd.readTable(); err != nil {
		return fmt.Errorf("queue.unbind read arguments: %w", err)
	}
	return nil
}

func (qu *QueueUnbind) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(qu.Reserved1); err != nil {
		return fmt.Errorf("queue.unbind write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(qu.Queue); err != nil {
		return fmt.Errorf("queue.unbind write queue: %w", err)
	}
	if err := wt.writeShortstr(qu.Exchange); err != nil {
		return fmt.Errorf("queue.unbind write exchange: %w", err)
	}
	if err := wt.writeShortstr(qu.RoutingKey); err != nil {
		return fmt.Errorf("queue.unbind write routing-key: %w", err)
	}
	if err := wt.writeTable(qu.Arguments); err != nil {
		return fmt.Errorf("queue.unbind write arguments: %w", err)
	}
	return nil
}

// QueueUnbindOk confirms the queue unbinding.
type QueueUnbindOk struct{}

func (*QueueUnbindOk) MethodID() uint32        { return MethodQueueUnbindOk }
func (*QueueUnbindOk) MethodName() string      { return "queue.unbind-ok" }
func (*QueueUnbindOk) Read(_ io.Reader) error  { return nil }
func (*QueueUnbindOk) Write(_ io.Writer) error { return nil }

// QueuePurge removes all messages from a queue.
type QueuePurge struct {
	Reserved1 uint16
	Queue     string // shortstr
	NoWait    bool
}

func (*QueuePurge) MethodID() uint32   { return MethodQueuePurge }
func (*QueuePurge) MethodName() string { return "queue.purge" }

func (qp *QueuePurge) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qp.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("queue.purge read reserved-1: %w", err)
	}
	if qp.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.purge read queue: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("queue.purge read bits: %w", err)
	}
	qp.NoWait = bits&1 != 0
	return nil
}

func (qp *QueuePurge) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(qp.Reserved1); err != nil {
		return fmt.Errorf("queue.purge write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(qp.Queue); err != nil {
		return fmt.Errorf("queue.purge write queue: %w", err)
	}
	var bits uint8
	if qp.NoWait {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("queue.purge write bits: %w", err)
	}
	return nil
}

// QueuePurgeOk confirms the queue purge.
type QueuePurgeOk struct {
	MessageCount uint32
}

func (*QueuePurgeOk) MethodID() uint32   { return MethodQueuePurgeOk }
func (*QueuePurgeOk) MethodName() string { return "queue.purge-ok" }

func (qp *QueuePurgeOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if qp.MessageCount, err = rd.readUint32(); err != nil {
		return fmt.Errorf("queue.purge-ok read message-count: %w", err)
	}
	return nil
}

func (qp *QueuePurgeOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeUint32(qp.MessageCount); err != nil {
		return fmt.Errorf("queue.purge-ok write message-count: %w", err)
	}
	return nil
}

// QueueDelete deletes a queue.
type QueueDelete struct {
	Reserved1 uint16
	Queue     string // shortstr
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (*QueueDelete) MethodID() uint32   { return MethodQueueDelete }
func (*QueueDelete) MethodName() string { return "queue.delete" }

func (qd *QueueDelete) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if qd.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("queue.delete read reserved-1: %w", err)
	}
	if qd.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("queue.delete read queue: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("queue.delete read bits: %w", err)
	}
	qd.IfUnused = bits&1 != 0
	qd.IfEmpty = bits&bit1 != 0
	qd.NoWait = bits&bit2 != 0
	return nil
}

func (qd *QueueDelete) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(qd.Reserved1); err != nil {
		return fmt.Errorf("queue.delete write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(qd.Queue); err != nil {
		return fmt.Errorf("queue.delete write queue: %w", err)
	}
	var bits uint8
	if qd.IfUnused {
		bits |= 1
	}
	if qd.IfEmpty {
		bits |= bit1
	}
	if qd.NoWait {
		bits |= bit2
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("queue.delete write bits: %w", err)
	}
	return nil
}

// QueueDeleteOk confirms the queue deletion.
type QueueDeleteOk struct {
	MessageCount uint32
}

func (*QueueDeleteOk) MethodID() uint32   { return MethodQueueDeleteOk }
func (*QueueDeleteOk) MethodName() string { return "queue.delete-ok" }

func (qd *QueueDeleteOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if qd.MessageCount, err = rd.readUint32(); err != nil {
		return fmt.Errorf("queue.delete-ok read message-count: %w", err)
	}
	return nil
}

func (qd *QueueDeleteOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeUint32(qd.MessageCount); err != nil {
		return fmt.Errorf("queue.delete-ok write message-count: %w", err)
	}
	return nil
}

// --- Basic methods ---

// BasicQos sets the quality of service.
type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (*BasicQos) MethodID() uint32   { return MethodBasicQos }
func (*BasicQos) MethodName() string { return "basic.qos" }

func (bq *BasicQos) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bq.PrefetchSize, err = rd.readUint32(); err != nil {
		return fmt.Errorf("basic.qos read prefetch-size: %w", err)
	}
	if bq.PrefetchCount, err = rd.readUint16(); err != nil {
		return fmt.Errorf("basic.qos read prefetch-count: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.qos read bits: %w", err)
	}
	bq.Global = bits&1 != 0
	return nil
}

func (bq *BasicQos) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint32(bq.PrefetchSize); err != nil {
		return fmt.Errorf("basic.qos write prefetch-size: %w", err)
	}
	if err := wt.writeUint16(bq.PrefetchCount); err != nil {
		return fmt.Errorf("basic.qos write prefetch-count: %w", err)
	}
	var bits uint8
	if bq.Global {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.qos write bits: %w", err)
	}
	return nil
}

// BasicQosOk confirms the QoS settings.
type BasicQosOk struct{}

func (*BasicQosOk) MethodID() uint32        { return MethodBasicQosOk }
func (*BasicQosOk) MethodName() string      { return "basic.qos-ok" }
func (*BasicQosOk) Read(_ io.Reader) error  { return nil }
func (*BasicQosOk) Write(_ io.Writer) error { return nil }

// BasicConsume starts a consumer.
type BasicConsume struct {
	Reserved1   uint16
	Queue       string // shortstr
	ConsumerTag string // shortstr
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   Table
}

func (*BasicConsume) MethodID() uint32   { return MethodBasicConsume }
func (*BasicConsume) MethodName() string { return "basic.consume" }

func (bc *BasicConsume) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bc.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("basic.consume read reserved-1: %w", err)
	}
	if bc.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.consume read queue: %w", err)
	}
	if bc.ConsumerTag, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.consume read consumer-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.consume read bits: %w", err)
	}
	bc.NoLocal = bits&1 != 0
	bc.NoAck = bits&bit1 != 0
	bc.Exclusive = bits&bit2 != 0
	bc.NoWait = bits&bit3 != 0
	if bc.Arguments, err = rd.readTable(); err != nil {
		return fmt.Errorf("basic.consume read arguments: %w", err)
	}
	return nil
}

func (bc *BasicConsume) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(bc.Reserved1); err != nil {
		return fmt.Errorf("basic.consume write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(bc.Queue); err != nil {
		return fmt.Errorf("basic.consume write queue: %w", err)
	}
	if err := wt.writeShortstr(bc.ConsumerTag); err != nil {
		return fmt.Errorf("basic.consume write consumer-tag: %w", err)
	}
	var bits uint8
	if bc.NoLocal {
		bits |= 1
	}
	if bc.NoAck {
		bits |= bit1
	}
	if bc.Exclusive {
		bits |= bit2
	}
	if bc.NoWait {
		bits |= bit3
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.consume write bits: %w", err)
	}
	if err := wt.writeTable(bc.Arguments); err != nil {
		return fmt.Errorf("basic.consume write arguments: %w", err)
	}
	return nil
}

// BasicConsumeOk confirms the consumer.
type BasicConsumeOk struct {
	ConsumerTag string // shortstr
}

func (*BasicConsumeOk) MethodID() uint32   { return MethodBasicConsumeOk }
func (*BasicConsumeOk) MethodName() string { return "basic.consume-ok" }

func (bc *BasicConsumeOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if bc.ConsumerTag, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.consume-ok read consumer-tag: %w", err)
	}
	return nil
}

func (bc *BasicConsumeOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeShortstr(bc.ConsumerTag); err != nil {
		return fmt.Errorf("basic.consume-ok write consumer-tag: %w", err)
	}
	return nil
}

// BasicCancel cancels a consumer.
type BasicCancel struct {
	ConsumerTag string // shortstr
	NoWait      bool
}

func (*BasicCancel) MethodID() uint32   { return MethodBasicCancel }
func (*BasicCancel) MethodName() string { return "basic.cancel" }

func (bc *BasicCancel) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if bc.ConsumerTag, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.cancel read consumer-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.cancel read bits: %w", err)
	}
	bc.NoWait = bits&1 != 0
	return nil
}

func (bc *BasicCancel) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeShortstr(bc.ConsumerTag); err != nil {
		return fmt.Errorf("basic.cancel write consumer-tag: %w", err)
	}
	var bits uint8
	if bc.NoWait {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.cancel write bits: %w", err)
	}
	return nil
}

// BasicCancelOk confirms the cancellation.
type BasicCancelOk struct {
	ConsumerTag string // shortstr
}

func (*BasicCancelOk) MethodID() uint32   { return MethodBasicCancelOk }
func (*BasicCancelOk) MethodName() string { return "basic.cancel-ok" }

func (bc *BasicCancelOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if bc.ConsumerTag, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.cancel-ok read consumer-tag: %w", err)
	}
	return nil
}

func (bc *BasicCancelOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeShortstr(bc.ConsumerTag); err != nil {
		return fmt.Errorf("basic.cancel-ok write consumer-tag: %w", err)
	}
	return nil
}

// BasicPublish publishes a message.
type BasicPublish struct {
	Reserved1  uint16
	Exchange   string // shortstr
	RoutingKey string // shortstr
	Mandatory  bool
	Immediate  bool
}

func (*BasicPublish) MethodID() uint32   { return MethodBasicPublish }
func (*BasicPublish) MethodName() string { return "basic.publish" }

func (bp *BasicPublish) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bp.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("basic.publish read reserved-1: %w", err)
	}
	if bp.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.publish read exchange: %w", err)
	}
	if bp.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.publish read routing-key: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.publish read bits: %w", err)
	}
	bp.Mandatory = bits&1 != 0
	bp.Immediate = bits&bit1 != 0
	return nil
}

func (bp *BasicPublish) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(bp.Reserved1); err != nil {
		return fmt.Errorf("basic.publish write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(bp.Exchange); err != nil {
		return fmt.Errorf("basic.publish write exchange: %w", err)
	}
	if err := wt.writeShortstr(bp.RoutingKey); err != nil {
		return fmt.Errorf("basic.publish write routing-key: %w", err)
	}
	var bits uint8
	if bp.Mandatory {
		bits |= 1
	}
	if bp.Immediate {
		bits |= bit1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.publish write bits: %w", err)
	}
	return nil
}

// BasicReturn returns an undeliverable message.
type BasicReturn struct {
	ReplyCode  uint16
	ReplyText  string // shortstr
	Exchange   string // shortstr
	RoutingKey string // shortstr
}

func (*BasicReturn) MethodID() uint32   { return MethodBasicReturn }
func (*BasicReturn) MethodName() string { return "basic.return" }

func (br *BasicReturn) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if br.ReplyCode, err = rd.readUint16(); err != nil {
		return fmt.Errorf("basic.return read reply-code: %w", err)
	}
	if br.ReplyText, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.return read reply-text: %w", err)
	}
	if br.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.return read exchange: %w", err)
	}
	if br.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.return read routing-key: %w", err)
	}
	return nil
}

func (br *BasicReturn) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(br.ReplyCode); err != nil {
		return fmt.Errorf("basic.return write reply-code: %w", err)
	}
	if err := wt.writeShortstr(br.ReplyText); err != nil {
		return fmt.Errorf("basic.return write reply-text: %w", err)
	}
	if err := wt.writeShortstr(br.Exchange); err != nil {
		return fmt.Errorf("basic.return write exchange: %w", err)
	}
	if err := wt.writeShortstr(br.RoutingKey); err != nil {
		return fmt.Errorf("basic.return write routing-key: %w", err)
	}
	return nil
}

// BasicDeliver delivers a message to a consumer.
type BasicDeliver struct {
	ConsumerTag string // shortstr
	DeliveryTag uint64
	Redelivered bool
	Exchange    string // shortstr
	RoutingKey  string // shortstr
}

func (*BasicDeliver) MethodID() uint32   { return MethodBasicDeliver }
func (*BasicDeliver) MethodName() string { return "basic.deliver" }

func (bd *BasicDeliver) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bd.ConsumerTag, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.deliver read consumer-tag: %w", err)
	}
	if bd.DeliveryTag, err = rd.readUint64(); err != nil {
		return fmt.Errorf("basic.deliver read delivery-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.deliver read bits: %w", err)
	}
	bd.Redelivered = bits&1 != 0
	if bd.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.deliver read exchange: %w", err)
	}
	if bd.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.deliver read routing-key: %w", err)
	}
	return nil
}

func (bd *BasicDeliver) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeShortstr(bd.ConsumerTag); err != nil {
		return fmt.Errorf("basic.deliver write consumer-tag: %w", err)
	}
	if err := wt.writeUint64(bd.DeliveryTag); err != nil {
		return fmt.Errorf("basic.deliver write delivery-tag: %w", err)
	}
	var bits uint8
	if bd.Redelivered {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.deliver write bits: %w", err)
	}
	if err := wt.writeShortstr(bd.Exchange); err != nil {
		return fmt.Errorf("basic.deliver write exchange: %w", err)
	}
	if err := wt.writeShortstr(bd.RoutingKey); err != nil {
		return fmt.Errorf("basic.deliver write routing-key: %w", err)
	}
	return nil
}

// BasicGet requests a single message.
type BasicGet struct {
	Reserved1 uint16
	Queue     string // shortstr
	NoAck     bool
}

func (*BasicGet) MethodID() uint32   { return MethodBasicGet }
func (*BasicGet) MethodName() string { return "basic.get" }

func (bg *BasicGet) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bg.Reserved1, err = rd.readUint16(); err != nil {
		return fmt.Errorf("basic.get read reserved-1: %w", err)
	}
	if bg.Queue, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.get read queue: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.get read bits: %w", err)
	}
	bg.NoAck = bits&1 != 0
	return nil
}

func (bg *BasicGet) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(bg.Reserved1); err != nil {
		return fmt.Errorf("basic.get write reserved-1: %w", err)
	}
	if err := wt.writeShortstr(bg.Queue); err != nil {
		return fmt.Errorf("basic.get write queue: %w", err)
	}
	var bits uint8
	if bg.NoAck {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.get write bits: %w", err)
	}
	return nil
}

// BasicGetOk returns a single message.
type BasicGetOk struct {
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string // shortstr
	RoutingKey   string // shortstr
	MessageCount uint32
}

func (*BasicGetOk) MethodID() uint32   { return MethodBasicGetOk }
func (*BasicGetOk) MethodName() string { return "basic.get-ok" }

func (bg *BasicGetOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bg.DeliveryTag, err = rd.readUint64(); err != nil {
		return fmt.Errorf("basic.get-ok read delivery-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.get-ok read bits: %w", err)
	}
	bg.Redelivered = bits&1 != 0
	if bg.Exchange, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.get-ok read exchange: %w", err)
	}
	if bg.RoutingKey, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.get-ok read routing-key: %w", err)
	}
	if bg.MessageCount, err = rd.readUint32(); err != nil {
		return fmt.Errorf("basic.get-ok read message-count: %w", err)
	}
	return nil
}

func (bg *BasicGetOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint64(bg.DeliveryTag); err != nil {
		return fmt.Errorf("basic.get-ok write delivery-tag: %w", err)
	}
	var bits uint8
	if bg.Redelivered {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.get-ok write bits: %w", err)
	}
	if err := wt.writeShortstr(bg.Exchange); err != nil {
		return fmt.Errorf("basic.get-ok write exchange: %w", err)
	}
	if err := wt.writeShortstr(bg.RoutingKey); err != nil {
		return fmt.Errorf("basic.get-ok write routing-key: %w", err)
	}
	if err := wt.writeUint32(bg.MessageCount); err != nil {
		return fmt.Errorf("basic.get-ok write message-count: %w", err)
	}
	return nil
}

// BasicGetEmpty indicates an empty queue.
type BasicGetEmpty struct {
	Reserved1 string // shortstr
}

func (*BasicGetEmpty) MethodID() uint32   { return MethodBasicGetEmpty }
func (*BasicGetEmpty) MethodName() string { return "basic.get-empty" }

func (bg *BasicGetEmpty) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	var err error
	if bg.Reserved1, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("basic.get-empty read reserved-1: %w", err)
	}
	return nil
}

func (bg *BasicGetEmpty) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	if err := wt.writeShortstr(bg.Reserved1); err != nil {
		return fmt.Errorf("basic.get-empty write reserved-1: %w", err)
	}
	return nil
}

// BasicAck acknowledges a message.
type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (*BasicAck) MethodID() uint32   { return MethodBasicAck }
func (*BasicAck) MethodName() string { return "basic.ack" }

func (ba *BasicAck) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if ba.DeliveryTag, err = rd.readUint64(); err != nil {
		return fmt.Errorf("basic.ack read delivery-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.ack read bits: %w", err)
	}
	ba.Multiple = bits&1 != 0
	return nil
}

func (ba *BasicAck) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint64(ba.DeliveryTag); err != nil {
		return fmt.Errorf("basic.ack write delivery-tag: %w", err)
	}
	var bits uint8
	if ba.Multiple {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.ack write bits: %w", err)
	}
	return nil
}

// BasicReject rejects a message.
type BasicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

func (*BasicReject) MethodID() uint32   { return MethodBasicReject }
func (*BasicReject) MethodName() string { return "basic.reject" }

func (br *BasicReject) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if br.DeliveryTag, err = rd.readUint64(); err != nil {
		return fmt.Errorf("basic.reject read delivery-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.reject read bits: %w", err)
	}
	br.Requeue = bits&1 != 0
	return nil
}

func (br *BasicReject) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint64(br.DeliveryTag); err != nil {
		return fmt.Errorf("basic.reject write delivery-tag: %w", err)
	}
	var bits uint8
	if br.Requeue {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.reject write bits: %w", err)
	}
	return nil
}

// BasicNack negatively acknowledges a message (RabbitMQ extension).
type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

func (*BasicNack) MethodID() uint32   { return MethodBasicNack }
func (*BasicNack) MethodName() string { return "basic.nack" }

func (bn *BasicNack) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if bn.DeliveryTag, err = rd.readUint64(); err != nil {
		return fmt.Errorf("basic.nack read delivery-tag: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.nack read bits: %w", err)
	}
	bn.Multiple = bits&1 != 0
	bn.Requeue = bits&bit1 != 0
	return nil
}

func (bn *BasicNack) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint64(bn.DeliveryTag); err != nil {
		return fmt.Errorf("basic.nack write delivery-tag: %w", err)
	}
	var bits uint8
	if bn.Multiple {
		bits |= 1
	}
	if bn.Requeue {
		bits |= bit1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.nack write bits: %w", err)
	}
	return nil
}

// BasicRecoverAsync requests redelivery (deprecated).
type BasicRecoverAsync struct {
	Requeue bool
}

func (*BasicRecoverAsync) MethodID() uint32   { return MethodBasicRecoverAsync }
func (*BasicRecoverAsync) MethodName() string { return "basic.recover-async" }

func (br *BasicRecoverAsync) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.recover-async read bits: %w", err)
	}
	br.Requeue = bits&1 != 0
	return nil
}

func (br *BasicRecoverAsync) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	var bits uint8
	if br.Requeue {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.recover-async write bits: %w", err)
	}
	return nil
}

// BasicRecover requests redelivery of unacknowledged messages.
type BasicRecover struct {
	Requeue bool
}

func (*BasicRecover) MethodID() uint32   { return MethodBasicRecover }
func (*BasicRecover) MethodName() string { return "basic.recover" }

func (br *BasicRecover) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("basic.recover read bits: %w", err)
	}
	br.Requeue = bits&1 != 0
	return nil
}

func (br *BasicRecover) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	var bits uint8
	if br.Requeue {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("basic.recover write bits: %w", err)
	}
	return nil
}

// BasicRecoverOk confirms the recovery.
type BasicRecoverOk struct{}

func (*BasicRecoverOk) MethodID() uint32        { return MethodBasicRecoverOk }
func (*BasicRecoverOk) MethodName() string      { return "basic.recover-ok" }
func (*BasicRecoverOk) Read(_ io.Reader) error  { return nil }
func (*BasicRecoverOk) Write(_ io.Writer) error { return nil }

// --- Tx methods ---

// TxSelect enables transactions.
type TxSelect struct{}

func (*TxSelect) MethodID() uint32        { return MethodTxSelect }
func (*TxSelect) MethodName() string      { return "tx.select" }
func (*TxSelect) Read(_ io.Reader) error  { return nil }
func (*TxSelect) Write(_ io.Writer) error { return nil }

// TxSelectOk confirms transaction mode.
type TxSelectOk struct{}

func (*TxSelectOk) MethodID() uint32        { return MethodTxSelectOk }
func (*TxSelectOk) MethodName() string      { return "tx.select-ok" }
func (*TxSelectOk) Read(_ io.Reader) error  { return nil }
func (*TxSelectOk) Write(_ io.Writer) error { return nil }

// TxCommit commits the current transaction.
type TxCommit struct{}

func (*TxCommit) MethodID() uint32        { return MethodTxCommit }
func (*TxCommit) MethodName() string      { return "tx.commit" }
func (*TxCommit) Read(_ io.Reader) error  { return nil }
func (*TxCommit) Write(_ io.Writer) error { return nil }

// TxCommitOk confirms the commit.
type TxCommitOk struct{}

func (*TxCommitOk) MethodID() uint32        { return MethodTxCommitOk }
func (*TxCommitOk) MethodName() string      { return "tx.commit-ok" }
func (*TxCommitOk) Read(_ io.Reader) error  { return nil }
func (*TxCommitOk) Write(_ io.Writer) error { return nil }

// TxRollback rolls back the current transaction.
type TxRollback struct{}

func (*TxRollback) MethodID() uint32        { return MethodTxRollback }
func (*TxRollback) MethodName() string      { return "tx.rollback" }
func (*TxRollback) Read(_ io.Reader) error  { return nil }
func (*TxRollback) Write(_ io.Writer) error { return nil }

// TxRollbackOk confirms the rollback.
type TxRollbackOk struct{}

func (*TxRollbackOk) MethodID() uint32        { return MethodTxRollbackOk }
func (*TxRollbackOk) MethodName() string      { return "tx.rollback-ok" }
func (*TxRollbackOk) Read(_ io.Reader) error  { return nil }
func (*TxRollbackOk) Write(_ io.Writer) error { return nil }

// --- Confirm methods ---

// ConfirmSelect enables publisher confirms.
type ConfirmSelect struct {
	NoWait bool
}

func (*ConfirmSelect) MethodID() uint32   { return MethodConfirmSelect }
func (*ConfirmSelect) MethodName() string { return "confirm.select" }

func (cs *ConfirmSelect) Read(r io.Reader) error {
	rd := &wireReader{rd: r}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("confirm.select read bits: %w", err)
	}
	cs.NoWait = bits&1 != 0
	return nil
}

func (cs *ConfirmSelect) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	var bits uint8
	if cs.NoWait {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("confirm.select write bits: %w", err)
	}
	return nil
}

// ConfirmSelectOk confirms publisher confirms mode.
type ConfirmSelectOk struct{}

func (*ConfirmSelectOk) MethodID() uint32        { return MethodConfirmSelectOk }
func (*ConfirmSelectOk) MethodName() string      { return "confirm.select-ok" }
func (*ConfirmSelectOk) Read(_ io.Reader) error  { return nil }
func (*ConfirmSelectOk) Write(_ io.Writer) error { return nil }
