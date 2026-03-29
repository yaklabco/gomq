package amqp

import (
	"fmt"
	"io"
)

// Method is the interface that all AMQP method frames implement.
type Method interface {
	MethodID() uint32
	MethodName() string
	Read(r io.Reader) error
	Write(w io.Writer) error
}

// methodFactory maps method IDs to factory functions that create zero-value Method instances.
//
//nolint:gochecknoglobals // read-only registry initialized once
var methodFactory = map[uint32]func() Method{
	// Connection.
	MethodConnectionStart:   func() Method { return &ConnectionStart{} },
	MethodConnectionStartOk: func() Method { return &ConnectionStartOk{} },
	MethodConnectionTune:    func() Method { return &ConnectionTune{} },
	MethodConnectionTuneOk:  func() Method { return &ConnectionTuneOk{} },
	MethodConnectionOpen:    func() Method { return &ConnectionOpen{} },
	MethodConnectionOpenOk:  func() Method { return &ConnectionOpenOk{} },
	MethodConnectionClose:   func() Method { return &ConnectionClose{} },
	MethodConnectionCloseOk: func() Method { return &ConnectionCloseOk{} },

	// Channel.
	MethodChannelOpen:    func() Method { return &ChannelOpen{} },
	MethodChannelOpenOk:  func() Method { return &ChannelOpenOk{} },
	MethodChannelFlow:    func() Method { return &ChannelFlow{} },
	MethodChannelFlowOk:  func() Method { return &ChannelFlowOk{} },
	MethodChannelClose:   func() Method { return &ChannelClose{} },
	MethodChannelCloseOk: func() Method { return &ChannelCloseOk{} },

	// Exchange.
	MethodExchangeDeclare:   func() Method { return &ExchangeDeclare{} },
	MethodExchangeDeclareOk: func() Method { return &ExchangeDeclareOk{} },
	MethodExchangeDelete:    func() Method { return &ExchangeDelete{} },
	MethodExchangeDeleteOk:  func() Method { return &ExchangeDeleteOk{} },

	// Queue.
	MethodQueueDeclare:   func() Method { return &QueueDeclare{} },
	MethodQueueDeclareOk: func() Method { return &QueueDeclareOk{} },
	MethodQueueBind:      func() Method { return &QueueBind{} },
	MethodQueueBindOk:    func() Method { return &QueueBindOk{} },
	MethodQueueUnbind:    func() Method { return &QueueUnbind{} },
	MethodQueueUnbindOk:  func() Method { return &QueueUnbindOk{} },
	MethodQueuePurge:     func() Method { return &QueuePurge{} },
	MethodQueuePurgeOk:   func() Method { return &QueuePurgeOk{} },
	MethodQueueDelete:    func() Method { return &QueueDelete{} },
	MethodQueueDeleteOk:  func() Method { return &QueueDeleteOk{} },

	// Basic.
	MethodBasicQos:          func() Method { return &BasicQos{} },
	MethodBasicQosOk:        func() Method { return &BasicQosOk{} },
	MethodBasicConsume:      func() Method { return &BasicConsume{} },
	MethodBasicConsumeOk:    func() Method { return &BasicConsumeOk{} },
	MethodBasicCancel:       func() Method { return &BasicCancel{} },
	MethodBasicCancelOk:     func() Method { return &BasicCancelOk{} },
	MethodBasicPublish:      func() Method { return &BasicPublish{} },
	MethodBasicReturn:       func() Method { return &BasicReturn{} },
	MethodBasicDeliver:      func() Method { return &BasicDeliver{} },
	MethodBasicGet:          func() Method { return &BasicGet{} },
	MethodBasicGetOk:        func() Method { return &BasicGetOk{} },
	MethodBasicGetEmpty:     func() Method { return &BasicGetEmpty{} },
	MethodBasicAck:          func() Method { return &BasicAck{} },
	MethodBasicReject:       func() Method { return &BasicReject{} },
	MethodBasicNack:         func() Method { return &BasicNack{} },
	MethodBasicRecoverAsync: func() Method { return &BasicRecoverAsync{} },
	MethodBasicRecover:      func() Method { return &BasicRecover{} },
	MethodBasicRecoverOk:    func() Method { return &BasicRecoverOk{} },

	// Tx.
	MethodTxSelect:     func() Method { return &TxSelect{} },
	MethodTxSelectOk:   func() Method { return &TxSelectOk{} },
	MethodTxCommit:     func() Method { return &TxCommit{} },
	MethodTxCommitOk:   func() Method { return &TxCommitOk{} },
	MethodTxRollback:   func() Method { return &TxRollback{} },
	MethodTxRollbackOk: func() Method { return &TxRollbackOk{} },

	// Confirm.
	MethodConfirmSelect:   func() Method { return &ConfirmSelect{} },
	MethodConfirmSelectOk: func() Method { return &ConfirmSelectOk{} },
}

// MethodByID returns a zero-value Method for the given method ID.
func MethodByID(id uint32) (Method, error) {
	factory, ok := methodFactory[id]
	if !ok {
		return nil, fmt.Errorf("unknown method ID: 0x%08X", id)
	}
	return factory(), nil
}

// --- Connection methods ---

// ConnectionStart is sent by the server to initiate the connection handshake.
type ConnectionStart struct {
	VersionMajor     uint8
	VersionMinor     uint8
	ServerProperties Table
	Mechanisms       []byte // longstr
	Locales          []byte // longstr
}

func (*ConnectionStart) MethodID() uint32   { return MethodConnectionStart }
func (*ConnectionStart) MethodName() string { return "connection.start" }

func (cs *ConnectionStart) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if cs.VersionMajor, err = rd.readUint8(); err != nil {
		return fmt.Errorf("connection.start read version-major: %w", err)
	}
	if cs.VersionMinor, err = rd.readUint8(); err != nil {
		return fmt.Errorf("connection.start read version-minor: %w", err)
	}
	if cs.ServerProperties, err = rd.readTable(); err != nil {
		return fmt.Errorf("connection.start read server-properties: %w", err)
	}
	if cs.Mechanisms, err = rd.readLongstr(); err != nil {
		return fmt.Errorf("connection.start read mechanisms: %w", err)
	}
	if cs.Locales, err = rd.readLongstr(); err != nil {
		return fmt.Errorf("connection.start read locales: %w", err)
	}
	return nil
}

func (cs *ConnectionStart) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint8(cs.VersionMajor); err != nil {
		return fmt.Errorf("connection.start write version-major: %w", err)
	}
	if err := wt.writeUint8(cs.VersionMinor); err != nil {
		return fmt.Errorf("connection.start write version-minor: %w", err)
	}
	if err := wt.writeTable(cs.ServerProperties); err != nil {
		return fmt.Errorf("connection.start write server-properties: %w", err)
	}
	if err := wt.writeLongstr(cs.Mechanisms); err != nil {
		return fmt.Errorf("connection.start write mechanisms: %w", err)
	}
	if err := wt.writeLongstr(cs.Locales); err != nil {
		return fmt.Errorf("connection.start write locales: %w", err)
	}
	return nil
}

// ConnectionStartOk is the client's response to ConnectionStart.
type ConnectionStartOk struct {
	ClientProperties Table
	Mechanism        string // shortstr
	Response         []byte // longstr
	Locale           string // shortstr
}

func (*ConnectionStartOk) MethodID() uint32   { return MethodConnectionStartOk }
func (*ConnectionStartOk) MethodName() string { return "connection.start-ok" }

func (cs *ConnectionStartOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if cs.ClientProperties, err = rd.readTable(); err != nil {
		return fmt.Errorf("connection.start-ok read client-properties: %w", err)
	}
	if cs.Mechanism, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.start-ok read mechanism: %w", err)
	}
	if cs.Response, err = rd.readLongstr(); err != nil {
		return fmt.Errorf("connection.start-ok read response: %w", err)
	}
	if cs.Locale, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.start-ok read locale: %w", err)
	}
	return nil
}

func (cs *ConnectionStartOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeTable(cs.ClientProperties); err != nil {
		return fmt.Errorf("connection.start-ok write client-properties: %w", err)
	}
	if err := wt.writeShortstr(cs.Mechanism); err != nil {
		return fmt.Errorf("connection.start-ok write mechanism: %w", err)
	}
	if err := wt.writeLongstr(cs.Response); err != nil {
		return fmt.Errorf("connection.start-ok write response: %w", err)
	}
	if err := wt.writeShortstr(cs.Locale); err != nil {
		return fmt.Errorf("connection.start-ok write locale: %w", err)
	}
	return nil
}

// ConnectionTune is sent by the server to negotiate connection parameters.
type ConnectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (*ConnectionTune) MethodID() uint32   { return MethodConnectionTune }
func (*ConnectionTune) MethodName() string { return "connection.tune" }

func (ct *ConnectionTune) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if ct.ChannelMax, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.tune read channel-max: %w", err)
	}
	if ct.FrameMax, err = rd.readUint32(); err != nil {
		return fmt.Errorf("connection.tune read frame-max: %w", err)
	}
	if ct.Heartbeat, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.tune read heartbeat: %w", err)
	}
	return nil
}

func (ct *ConnectionTune) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(ct.ChannelMax); err != nil {
		return fmt.Errorf("connection.tune write channel-max: %w", err)
	}
	if err := wt.writeUint32(ct.FrameMax); err != nil {
		return fmt.Errorf("connection.tune write frame-max: %w", err)
	}
	if err := wt.writeUint16(ct.Heartbeat); err != nil {
		return fmt.Errorf("connection.tune write heartbeat: %w", err)
	}
	return nil
}

// ConnectionTuneOk is the client's response to ConnectionTune.
type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (*ConnectionTuneOk) MethodID() uint32   { return MethodConnectionTuneOk }
func (*ConnectionTuneOk) MethodName() string { return "connection.tune-ok" }

func (ct *ConnectionTuneOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if ct.ChannelMax, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.tune-ok read channel-max: %w", err)
	}
	if ct.FrameMax, err = rd.readUint32(); err != nil {
		return fmt.Errorf("connection.tune-ok read frame-max: %w", err)
	}
	if ct.Heartbeat, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.tune-ok read heartbeat: %w", err)
	}
	return nil
}

func (ct *ConnectionTuneOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(ct.ChannelMax); err != nil {
		return fmt.Errorf("connection.tune-ok write channel-max: %w", err)
	}
	if err := wt.writeUint32(ct.FrameMax); err != nil {
		return fmt.Errorf("connection.tune-ok write frame-max: %w", err)
	}
	if err := wt.writeUint16(ct.Heartbeat); err != nil {
		return fmt.Errorf("connection.tune-ok write heartbeat: %w", err)
	}
	return nil
}

// ConnectionOpen requests access to a virtual host.
type ConnectionOpen struct {
	VirtualHost string // shortstr
	Reserved1   string // shortstr
	Reserved2   bool
}

func (*ConnectionOpen) MethodID() uint32   { return MethodConnectionOpen }
func (*ConnectionOpen) MethodName() string { return "connection.open" }

func (co *ConnectionOpen) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if co.VirtualHost, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.open read virtual-host: %w", err)
	}
	if co.Reserved1, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.open read reserved-1: %w", err)
	}
	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("connection.open read reserved-2: %w", err)
	}
	co.Reserved2 = bits&1 != 0
	return nil
}

func (co *ConnectionOpen) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeShortstr(co.VirtualHost); err != nil {
		return fmt.Errorf("connection.open write virtual-host: %w", err)
	}
	if err := wt.writeShortstr(co.Reserved1); err != nil {
		return fmt.Errorf("connection.open write reserved-1: %w", err)
	}
	var bits uint8
	if co.Reserved2 {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("connection.open write reserved-2: %w", err)
	}
	return nil
}

// ConnectionOpenOk confirms the virtual host is accessible.
type ConnectionOpenOk struct {
	Reserved1 string // shortstr
}

func (*ConnectionOpenOk) MethodID() uint32   { return MethodConnectionOpenOk }
func (*ConnectionOpenOk) MethodName() string { return "connection.open-ok" }

func (co *ConnectionOpenOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if co.Reserved1, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.open-ok read reserved-1: %w", err)
	}
	return nil
}

func (co *ConnectionOpenOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeShortstr(co.Reserved1); err != nil {
		return fmt.Errorf("connection.open-ok write reserved-1: %w", err)
	}
	return nil
}

// ConnectionClose requests a graceful connection shutdown.
type ConnectionClose struct {
	ReplyCode       uint16
	ReplyText       string // shortstr
	FailingClassID  uint16
	FailingMethodID uint16
}

func (*ConnectionClose) MethodID() uint32   { return MethodConnectionClose }
func (*ConnectionClose) MethodName() string { return "connection.close" }

func (cc *ConnectionClose) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if cc.ReplyCode, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.close read reply-code: %w", err)
	}
	if cc.ReplyText, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("connection.close read reply-text: %w", err)
	}
	if cc.FailingClassID, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.close read class-id: %w", err)
	}
	if cc.FailingMethodID, err = rd.readUint16(); err != nil {
		return fmt.Errorf("connection.close read method-id: %w", err)
	}
	return nil
}

func (cc *ConnectionClose) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(cc.ReplyCode); err != nil {
		return fmt.Errorf("connection.close write reply-code: %w", err)
	}
	if err := wt.writeShortstr(cc.ReplyText); err != nil {
		return fmt.Errorf("connection.close write reply-text: %w", err)
	}
	if err := wt.writeUint16(cc.FailingClassID); err != nil {
		return fmt.Errorf("connection.close write class-id: %w", err)
	}
	if err := wt.writeUint16(cc.FailingMethodID); err != nil {
		return fmt.Errorf("connection.close write method-id: %w", err)
	}
	return nil
}

// ConnectionCloseOk confirms the connection close.
type ConnectionCloseOk struct{}

func (*ConnectionCloseOk) MethodID() uint32        { return MethodConnectionCloseOk }
func (*ConnectionCloseOk) MethodName() string      { return "connection.close-ok" }
func (*ConnectionCloseOk) Read(_ io.Reader) error  { return nil }
func (*ConnectionCloseOk) Write(_ io.Writer) error { return nil }

// --- Channel methods ---

// ChannelOpen opens a new channel.
type ChannelOpen struct {
	Reserved1 string // shortstr
}

func (*ChannelOpen) MethodID() uint32   { return MethodChannelOpen }
func (*ChannelOpen) MethodName() string { return "channel.open" }

func (co *ChannelOpen) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if co.Reserved1, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("channel.open read reserved-1: %w", err)
	}
	return nil
}

func (co *ChannelOpen) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeShortstr(co.Reserved1); err != nil {
		return fmt.Errorf("channel.open write reserved-1: %w", err)
	}
	return nil
}

// ChannelOpenOk confirms the channel is open.
type ChannelOpenOk struct {
	Reserved1 []byte // longstr
}

func (*ChannelOpenOk) MethodID() uint32   { return MethodChannelOpenOk }
func (*ChannelOpenOk) MethodName() string { return "channel.open-ok" }

func (co *ChannelOpenOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if co.Reserved1, err = rd.readLongstr(); err != nil {
		return fmt.Errorf("channel.open-ok read reserved-1: %w", err)
	}
	return nil
}

func (co *ChannelOpenOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeLongstr(co.Reserved1); err != nil {
		return fmt.Errorf("channel.open-ok write reserved-1: %w", err)
	}
	return nil
}

// ChannelClose requests a channel shutdown.
type ChannelClose struct {
	ReplyCode       uint16
	ReplyText       string // shortstr
	FailingClassID  uint16
	FailingMethodID uint16
}

func (*ChannelClose) MethodID() uint32   { return MethodChannelClose }
func (*ChannelClose) MethodName() string { return "channel.close" }

func (cc *ChannelClose) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	var err error
	if cc.ReplyCode, err = rd.readUint16(); err != nil {
		return fmt.Errorf("channel.close read reply-code: %w", err)
	}
	if cc.ReplyText, err = rd.readShortstr(); err != nil {
		return fmt.Errorf("channel.close read reply-text: %w", err)
	}
	if cc.FailingClassID, err = rd.readUint16(); err != nil {
		return fmt.Errorf("channel.close read class-id: %w", err)
	}
	if cc.FailingMethodID, err = rd.readUint16(); err != nil {
		return fmt.Errorf("channel.close read method-id: %w", err)
	}
	return nil
}

func (cc *ChannelClose) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	if err := wt.writeUint16(cc.ReplyCode); err != nil {
		return fmt.Errorf("channel.close write reply-code: %w", err)
	}
	if err := wt.writeShortstr(cc.ReplyText); err != nil {
		return fmt.Errorf("channel.close write reply-text: %w", err)
	}
	if err := wt.writeUint16(cc.FailingClassID); err != nil {
		return fmt.Errorf("channel.close write class-id: %w", err)
	}
	if err := wt.writeUint16(cc.FailingMethodID); err != nil {
		return fmt.Errorf("channel.close write method-id: %w", err)
	}
	return nil
}

// ChannelCloseOk confirms the channel close.
type ChannelCloseOk struct{}

func (*ChannelCloseOk) MethodID() uint32        { return MethodChannelCloseOk }
func (*ChannelCloseOk) MethodName() string      { return "channel.close-ok" }
func (*ChannelCloseOk) Read(_ io.Reader) error  { return nil }
func (*ChannelCloseOk) Write(_ io.Writer) error { return nil }

// ChannelFlow enables or disables flow control.
type ChannelFlow struct {
	Active bool
}

func (*ChannelFlow) MethodID() uint32   { return MethodChannelFlow }
func (*ChannelFlow) MethodName() string { return "channel.flow" }

func (cf *ChannelFlow) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("channel.flow read active: %w", err)
	}
	cf.Active = bits&1 != 0
	return nil
}

func (cf *ChannelFlow) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	var bits uint8
	if cf.Active {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("channel.flow write active: %w", err)
	}
	return nil
}

// ChannelFlowOk confirms the flow control state.
type ChannelFlowOk struct {
	Active bool
}

func (*ChannelFlowOk) MethodID() uint32   { return MethodChannelFlowOk }
func (*ChannelFlowOk) MethodName() string { return "channel.flow-ok" }

func (cf *ChannelFlowOk) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	bits, err := rd.readUint8()
	if err != nil {
		return fmt.Errorf("channel.flow-ok read active: %w", err)
	}
	cf.Active = bits&1 != 0
	return nil
}

func (cf *ChannelFlowOk) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}

	var bits uint8
	if cf.Active {
		bits |= 1
	}
	if err := wt.writeUint8(bits); err != nil {
		return fmt.Errorf("channel.flow-ok write active: %w", err)
	}
	return nil
}
