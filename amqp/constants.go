// Package amqp implements the AMQP 0-9-1 wire protocol codec.
package amqp

// Frame types.
const (
	FrameMethod    uint8  = 1
	FrameHeader    uint8  = 2
	FrameBody      uint8  = 3
	FrameHeartbeat uint8  = 8
	FrameEnd       byte   = 0xCE
	FrameMinSize   uint32 = 4096
)

// Reply codes.
const (
	ReplySuccess       uint16 = 200
	ContentTooLarge    uint16 = 311
	NoRoute            uint16 = 312
	NoConsumers        uint16 = 313
	ConnectionForced   uint16 = 320
	InvalidPath        uint16 = 402
	AccessRefused      uint16 = 403
	NotFound           uint16 = 404
	ResourceLocked     uint16 = 405
	PreconditionFailed uint16 = 406
	FrameError         uint16 = 501
	SyntaxError        uint16 = 502
	CommandInvalid     uint16 = 503
	ChannelError       uint16 = 504
	UnexpectedFrame    uint16 = 505
	ResourceError      uint16 = 506
	NotAllowed         uint16 = 530
	NotImplemented     uint16 = 540
	InternalError      uint16 = 541
)

// Class IDs.
const (
	ClassConnection uint16 = 10
	ClassChannel    uint16 = 20
	ClassExchange   uint16 = 40
	ClassQueue      uint16 = 50
	ClassBasic      uint16 = 60
	ClassConfirm    uint16 = 85
	ClassTx         uint16 = 90
)

// Method IDs encoded as (classID << 16) | methodID.
const (
	// Connection methods.
	MethodConnectionStart     uint32 = 0x000A000A
	MethodConnectionStartOk   uint32 = 0x000A000B
	MethodConnectionTune      uint32 = 0x000A001E
	MethodConnectionTuneOk    uint32 = 0x000A001F
	MethodConnectionOpen      uint32 = 0x000A0028
	MethodConnectionOpenOk    uint32 = 0x000A0029
	MethodConnectionClose     uint32 = 0x000A0032
	MethodConnectionCloseOk   uint32 = 0x000A0033
	MethodConnectionBlocked   uint32 = 0x000A003C // class 10, method 60
	MethodConnectionUnblocked uint32 = 0x000A003D // class 10, method 61

	// Channel methods.
	MethodChannelOpen    uint32 = 0x0014000A
	MethodChannelOpenOk  uint32 = 0x0014000B
	MethodChannelFlow    uint32 = 0x00140014
	MethodChannelFlowOk  uint32 = 0x00140015
	MethodChannelClose   uint32 = 0x00140028
	MethodChannelCloseOk uint32 = 0x00140029

	// Exchange methods.
	MethodExchangeDeclare   uint32 = 0x0028000A
	MethodExchangeDeclareOk uint32 = 0x0028000B
	MethodExchangeDelete    uint32 = 0x00280014
	MethodExchangeDeleteOk  uint32 = 0x00280015

	// Queue methods.
	MethodQueueDeclare   uint32 = 0x0032000A
	MethodQueueDeclareOk uint32 = 0x0032000B
	MethodQueueBind      uint32 = 0x00320014
	MethodQueueBindOk    uint32 = 0x00320015
	MethodQueuePurge     uint32 = 0x0032001E
	MethodQueuePurgeOk   uint32 = 0x0032001F
	MethodQueueDelete    uint32 = 0x00320028
	MethodQueueDeleteOk  uint32 = 0x00320029
	MethodQueueUnbind    uint32 = 0x00320032
	MethodQueueUnbindOk  uint32 = 0x00320033

	// Basic methods.
	MethodBasicQos          uint32 = 0x003C000A
	MethodBasicQosOk        uint32 = 0x003C000B
	MethodBasicConsume      uint32 = 0x003C0014
	MethodBasicConsumeOk    uint32 = 0x003C0015
	MethodBasicCancel       uint32 = 0x003C001E
	MethodBasicCancelOk     uint32 = 0x003C001F
	MethodBasicPublish      uint32 = 0x003C0028
	MethodBasicReturn       uint32 = 0x003C0032
	MethodBasicDeliver      uint32 = 0x003C003C
	MethodBasicGet          uint32 = 0x003C0046
	MethodBasicGetOk        uint32 = 0x003C0047
	MethodBasicGetEmpty     uint32 = 0x003C0048
	MethodBasicAck          uint32 = 0x003C0050
	MethodBasicReject       uint32 = 0x003C005A
	MethodBasicRecoverAsync uint32 = 0x003C0064
	MethodBasicRecover      uint32 = 0x003C006E
	MethodBasicRecoverOk    uint32 = 0x003C006F
	MethodBasicNack         uint32 = 0x003C0078

	// Tx methods.
	MethodTxSelect     uint32 = 0x005A000A
	MethodTxSelectOk   uint32 = 0x005A000B
	MethodTxCommit     uint32 = 0x005A0014
	MethodTxCommitOk   uint32 = 0x005A0015
	MethodTxRollback   uint32 = 0x005A001E
	MethodTxRollbackOk uint32 = 0x005A001F

	// Confirm methods.
	MethodConfirmSelect   uint32 = 0x0055000A
	MethodConfirmSelectOk uint32 = 0x0055000B
)
