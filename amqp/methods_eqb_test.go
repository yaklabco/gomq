package amqp

import (
	"bytes"
	"testing"
)

func TestBasicPublish_roundTrip(t *testing.T) {
	t.Parallel()

	original := &BasicPublish{
		Reserved1:  0,
		Exchange:   "test-exchange",
		RoutingKey: "test.key",
		Mandatory:  true,
		Immediate:  true,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &BasicPublish{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Exchange != original.Exchange {
		t.Errorf("Exchange = %q, want %q", decoded.Exchange, original.Exchange)
	}
	if decoded.RoutingKey != original.RoutingKey {
		t.Errorf("RoutingKey = %q, want %q", decoded.RoutingKey, original.RoutingKey)
	}
	if decoded.Mandatory != original.Mandatory {
		t.Errorf("Mandatory = %v, want %v", decoded.Mandatory, original.Mandatory)
	}
	if decoded.Immediate != original.Immediate {
		t.Errorf("Immediate = %v, want %v", decoded.Immediate, original.Immediate)
	}
}

func TestBasicPublish_bitPacking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mandatory bool
		immediate bool
		wantBits  byte
	}{
		{"neither", false, false, 0x00},
		{"mandatory only", true, false, 0x01},
		{"immediate only", false, true, 0x02},
		{"both", true, true, 0x03},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := &BasicPublish{
				Exchange:   "",
				RoutingKey: "",
				Mandatory:  tt.mandatory,
				Immediate:  tt.immediate,
			}

			var buf bytes.Buffer
			if err := original.Write(&buf); err != nil {
				t.Fatalf("Write() error = %v", err)
			}

			// Decode and verify the bits round-trip correctly.
			decoded := &BasicPublish{}
			if err := decoded.Read(bytes.NewReader(buf.Bytes())); err != nil {
				t.Fatalf("Read() error = %v", err)
			}

			if decoded.Mandatory != tt.mandatory {
				t.Errorf("Mandatory = %v, want %v", decoded.Mandatory, tt.mandatory)
			}
			if decoded.Immediate != tt.immediate {
				t.Errorf("Immediate = %v, want %v", decoded.Immediate, tt.immediate)
			}
		})
	}
}

func TestBasicDeliver_roundTrip(t *testing.T) {
	t.Parallel()

	original := &BasicDeliver{
		ConsumerTag: "ctag-1",
		DeliveryTag: 42,
		Redelivered: true,
		Exchange:    "amq.direct",
		RoutingKey:  "my.key",
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &BasicDeliver{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ConsumerTag != original.ConsumerTag {
		t.Errorf("ConsumerTag = %q, want %q", decoded.ConsumerTag, original.ConsumerTag)
	}
	if decoded.DeliveryTag != original.DeliveryTag {
		t.Errorf("DeliveryTag = %d, want %d", decoded.DeliveryTag, original.DeliveryTag)
	}
	if decoded.Redelivered != original.Redelivered {
		t.Errorf("Redelivered = %v, want %v", decoded.Redelivered, original.Redelivered)
	}
	if decoded.Exchange != original.Exchange {
		t.Errorf("Exchange = %q, want %q", decoded.Exchange, original.Exchange)
	}
	if decoded.RoutingKey != original.RoutingKey {
		t.Errorf("RoutingKey = %q, want %q", decoded.RoutingKey, original.RoutingKey)
	}
}

func TestBasicAck_roundTrip(t *testing.T) {
	t.Parallel()

	original := &BasicAck{
		DeliveryTag: 99,
		Multiple:    true,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &BasicAck{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.DeliveryTag != original.DeliveryTag {
		t.Errorf("DeliveryTag = %d, want %d", decoded.DeliveryTag, original.DeliveryTag)
	}
	if decoded.Multiple != original.Multiple {
		t.Errorf("Multiple = %v, want %v", decoded.Multiple, original.Multiple)
	}
}

func TestQueueDeclare_roundTrip(t *testing.T) {
	t.Parallel()

	original := &QueueDeclare{
		Queue:      "test-queue",
		Passive:    false,
		Durable:    true,
		Exclusive:  false,
		AutoDelete: true,
		NoWait:     false,
		Arguments:  Table{"x-max-length": int32(1000)},
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &QueueDeclare{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Queue != original.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, original.Queue)
	}
	if decoded.Durable != original.Durable {
		t.Errorf("Durable = %v, want %v", decoded.Durable, original.Durable)
	}
	if decoded.AutoDelete != original.AutoDelete {
		t.Errorf("AutoDelete = %v, want %v", decoded.AutoDelete, original.AutoDelete)
	}
	maxLen, ok := decoded.Arguments["x-max-length"].(int32)
	if !ok || maxLen != 1000 {
		t.Errorf("Arguments[x-max-length] = %v, want int32(1000)", decoded.Arguments["x-max-length"])
	}
}

func TestExchangeDeclare_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ExchangeDeclare{
		Exchange:   "test-exchange",
		Type:       "topic",
		Passive:    false,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  Table{},
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ExchangeDeclare{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Exchange != original.Exchange {
		t.Errorf("Exchange = %q, want %q", decoded.Exchange, original.Exchange)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.Durable != original.Durable {
		t.Errorf("Durable = %v, want %v", decoded.Durable, original.Durable)
	}
}

func TestBasicNack_roundTrip(t *testing.T) {
	t.Parallel()

	original := &BasicNack{
		DeliveryTag: 55,
		Multiple:    true,
		Requeue:     true,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &BasicNack{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.DeliveryTag != original.DeliveryTag {
		t.Errorf("DeliveryTag = %d, want %d", decoded.DeliveryTag, original.DeliveryTag)
	}
	if decoded.Multiple != original.Multiple {
		t.Errorf("Multiple = %v, want %v", decoded.Multiple, original.Multiple)
	}
	if decoded.Requeue != original.Requeue {
		t.Errorf("Requeue = %v, want %v", decoded.Requeue, original.Requeue)
	}
}

func TestMethodByID_allEQBMethods(t *testing.T) {
	t.Parallel()

	ids := []uint32{
		MethodExchangeDeclare, MethodExchangeDeclareOk,
		MethodExchangeDelete, MethodExchangeDeleteOk,
		MethodQueueDeclare, MethodQueueDeclareOk,
		MethodQueueBind, MethodQueueBindOk,
		MethodQueueUnbind, MethodQueueUnbindOk,
		MethodQueuePurge, MethodQueuePurgeOk,
		MethodQueueDelete, MethodQueueDeleteOk,
		MethodBasicQos, MethodBasicQosOk,
		MethodBasicConsume, MethodBasicConsumeOk,
		MethodBasicCancel, MethodBasicCancelOk,
		MethodBasicPublish,
		MethodBasicReturn,
		MethodBasicDeliver,
		MethodBasicGet, MethodBasicGetOk, MethodBasicGetEmpty,
		MethodBasicAck, MethodBasicReject, MethodBasicNack,
		MethodBasicRecoverAsync, MethodBasicRecover, MethodBasicRecoverOk,
		MethodTxSelect, MethodTxSelectOk,
		MethodTxCommit, MethodTxCommitOk,
		MethodTxRollback, MethodTxRollbackOk,
		MethodConfirmSelect, MethodConfirmSelectOk,
	}
	for _, id := range ids {
		t.Run(methodName(id), func(t *testing.T) {
			t.Parallel()
			method, err := MethodByID(id)
			if err != nil {
				t.Fatalf("MethodByID(0x%08X) error = %v", id, err)
			}
			if method.MethodID() != id {
				t.Errorf("MethodID() = 0x%08X, want 0x%08X", method.MethodID(), id)
			}
		})
	}
}

func TestQueueBind_roundTrip(t *testing.T) {
	t.Parallel()

	original := &QueueBind{
		Queue:      "test-queue",
		Exchange:   "test-exchange",
		RoutingKey: "test.#",
		NoWait:     false,
		Arguments:  Table{},
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &QueueBind{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Queue != original.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, original.Queue)
	}
	if decoded.Exchange != original.Exchange {
		t.Errorf("Exchange = %q, want %q", decoded.Exchange, original.Exchange)
	}
	if decoded.RoutingKey != original.RoutingKey {
		t.Errorf("RoutingKey = %q, want %q", decoded.RoutingKey, original.RoutingKey)
	}
}

func TestBasicConsume_roundTrip(t *testing.T) {
	t.Parallel()

	original := &BasicConsume{
		Queue:       "test-queue",
		ConsumerTag: "ctag-1",
		NoLocal:     false,
		NoAck:       true,
		Exclusive:   false,
		NoWait:      false,
		Arguments:   Table{},
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &BasicConsume{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Queue != original.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, original.Queue)
	}
	if decoded.ConsumerTag != original.ConsumerTag {
		t.Errorf("ConsumerTag = %q, want %q", decoded.ConsumerTag, original.ConsumerTag)
	}
	if decoded.NoAck != original.NoAck {
		t.Errorf("NoAck = %v, want %v", decoded.NoAck, original.NoAck)
	}
}
