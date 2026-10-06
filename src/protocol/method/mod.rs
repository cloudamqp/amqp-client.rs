pub mod basic;
pub mod channel;
pub mod confirm;
pub mod connection;
pub mod exchange;
pub mod queue;
pub mod tx;

use bytes::{Bytes, BytesMut};

use super::types::{AmqpDecode, AmqpEncode};
use crate::error::ProtocolError;

/// All AMQP method frames, dispatched by (class_id, method_id).
#[derive(Clone, Debug, PartialEq)]
pub enum MethodFrame {
    // Connection (class 10)
    ConnectionStart(connection::Start),
    ConnectionStartOk(connection::StartOk),
    ConnectionSecure(connection::Secure),
    ConnectionSecureOk(connection::SecureOk),
    ConnectionTune(connection::Tune),
    ConnectionTuneOk(connection::TuneOk),
    ConnectionOpen(connection::Open),
    ConnectionOpenOk,
    ConnectionClose(connection::Close),
    ConnectionCloseOk,
    ConnectionBlocked(connection::Blocked),
    ConnectionUnblocked,

    // Channel (class 20)
    ChannelOpen,
    ChannelOpenOk,
    ChannelFlow(channel::Flow),
    ChannelFlowOk(channel::FlowOk),
    ChannelClose(channel::Close),
    ChannelCloseOk,

    // Exchange (class 40)
    ExchangeDeclare(exchange::Declare),
    ExchangeDeclareOk,
    ExchangeDelete(exchange::Delete),
    ExchangeDeleteOk,
    ExchangeBind(exchange::Bind),
    ExchangeBindOk,
    ExchangeUnbind(exchange::Unbind),
    ExchangeUnbindOk,

    // Queue (class 50)
    QueueDeclare(queue::Declare),
    QueueDeclareOk(queue::DeclareOk),
    QueueBind(queue::Bind),
    QueueBindOk,
    QueuePurge(queue::Purge),
    QueuePurgeOk(queue::PurgeOk),
    QueueDelete(queue::Delete),
    QueueDeleteOk(queue::DeleteOk),
    QueueUnbind(queue::Unbind),
    QueueUnbindOk,

    // Basic (class 60)
    BasicQos(basic::Qos),
    BasicQosOk,
    BasicConsume(basic::Consume),
    BasicConsumeOk(basic::ConsumeOk),
    BasicCancel(basic::Cancel),
    BasicCancelOk(basic::CancelOk),
    BasicPublish(basic::Publish),
    BasicReturn(basic::Return),
    BasicDeliver(basic::Deliver),
    BasicGet(basic::Get),
    BasicGetOk(basic::GetOk),
    BasicGetEmpty,
    BasicAck(basic::Ack),
    BasicReject(basic::Reject),
    BasicRecoverAsync(basic::RecoverAsync),
    BasicRecover(basic::Recover),
    BasicRecoverOk,
    BasicNack(basic::Nack),

    // Confirm (class 85)
    ConfirmSelect(confirm::Select),
    ConfirmSelectOk,

    // Tx (class 90)
    TxSelect,
    TxSelectOk,
    TxCommit,
    TxCommitOk,
    TxRollback,
    TxRollbackOk,
}

impl MethodFrame {
    pub fn class_id(&self) -> u16 {
        match self {
            Self::ConnectionStart(_)
            | Self::ConnectionStartOk(_)
            | Self::ConnectionSecure(_)
            | Self::ConnectionSecureOk(_)
            | Self::ConnectionTune(_)
            | Self::ConnectionTuneOk(_)
            | Self::ConnectionOpen(_)
            | Self::ConnectionOpenOk
            | Self::ConnectionClose(_)
            | Self::ConnectionCloseOk
            | Self::ConnectionBlocked(_)
            | Self::ConnectionUnblocked => 10,

            Self::ChannelOpen
            | Self::ChannelOpenOk
            | Self::ChannelFlow(_)
            | Self::ChannelFlowOk(_)
            | Self::ChannelClose(_)
            | Self::ChannelCloseOk => 20,

            Self::ExchangeDeclare(_)
            | Self::ExchangeDeclareOk
            | Self::ExchangeDelete(_)
            | Self::ExchangeDeleteOk
            | Self::ExchangeBind(_)
            | Self::ExchangeBindOk
            | Self::ExchangeUnbind(_)
            | Self::ExchangeUnbindOk => 40,

            Self::QueueDeclare(_)
            | Self::QueueDeclareOk(_)
            | Self::QueueBind(_)
            | Self::QueueBindOk
            | Self::QueuePurge(_)
            | Self::QueuePurgeOk(_)
            | Self::QueueDelete(_)
            | Self::QueueDeleteOk(_)
            | Self::QueueUnbind(_)
            | Self::QueueUnbindOk => 50,

            Self::BasicQos(_)
            | Self::BasicQosOk
            | Self::BasicConsume(_)
            | Self::BasicConsumeOk(_)
            | Self::BasicCancel(_)
            | Self::BasicCancelOk(_)
            | Self::BasicPublish(_)
            | Self::BasicReturn(_)
            | Self::BasicDeliver(_)
            | Self::BasicGet(_)
            | Self::BasicGetOk(_)
            | Self::BasicGetEmpty
            | Self::BasicAck(_)
            | Self::BasicReject(_)
            | Self::BasicRecoverAsync(_)
            | Self::BasicRecover(_)
            | Self::BasicRecoverOk
            | Self::BasicNack(_) => 60,

            Self::ConfirmSelect(_) | Self::ConfirmSelectOk => 85,

            Self::TxSelect
            | Self::TxSelectOk
            | Self::TxCommit
            | Self::TxCommitOk
            | Self::TxRollback
            | Self::TxRollbackOk => 90,
        }
    }

    pub fn method_id(&self) -> u16 {
        match self {
            Self::ConnectionStart(_) => 10,
            Self::ConnectionStartOk(_) => 11,
            Self::ConnectionSecure(_) => 20,
            Self::ConnectionSecureOk(_) => 21,
            Self::ConnectionTune(_) => 30,
            Self::ConnectionTuneOk(_) => 31,
            Self::ConnectionOpen(_) => 40,
            Self::ConnectionOpenOk => 41,
            Self::ConnectionClose(_) => 50,
            Self::ConnectionCloseOk => 51,
            Self::ConnectionBlocked(_) => 60,
            Self::ConnectionUnblocked => 61,

            Self::ChannelOpen => 10,
            Self::ChannelOpenOk => 11,
            Self::ChannelFlow(_) => 20,
            Self::ChannelFlowOk(_) => 21,
            Self::ChannelClose(_) => 40,
            Self::ChannelCloseOk => 41,

            Self::ExchangeDeclare(_) => 10,
            Self::ExchangeDeclareOk => 11,
            Self::ExchangeDelete(_) => 20,
            Self::ExchangeDeleteOk => 21,
            Self::ExchangeBind(_) => 30,
            Self::ExchangeBindOk => 31,
            Self::ExchangeUnbind(_) => 40,
            Self::ExchangeUnbindOk => 51,

            Self::QueueDeclare(_) => 10,
            Self::QueueDeclareOk(_) => 11,
            Self::QueueBind(_) => 20,
            Self::QueueBindOk => 21,
            Self::QueuePurge(_) => 30,
            Self::QueuePurgeOk(_) => 31,
            Self::QueueDelete(_) => 40,
            Self::QueueDeleteOk(_) => 41,
            Self::QueueUnbind(_) => 50,
            Self::QueueUnbindOk => 51,

            Self::BasicQos(_) => 10,
            Self::BasicQosOk => 11,
            Self::BasicConsume(_) => 20,
            Self::BasicConsumeOk(_) => 21,
            Self::BasicCancel(_) => 30,
            Self::BasicCancelOk(_) => 31,
            Self::BasicPublish(_) => 40,
            Self::BasicReturn(_) => 50,
            Self::BasicDeliver(_) => 60,
            Self::BasicGet(_) => 70,
            Self::BasicGetOk(_) => 71,
            Self::BasicGetEmpty => 72,
            Self::BasicAck(_) => 80,
            Self::BasicReject(_) => 90,
            Self::BasicRecoverAsync(_) => 100,
            Self::BasicRecover(_) => 110,
            Self::BasicRecoverOk => 111,
            Self::BasicNack(_) => 120,

            Self::ConfirmSelect(_) => 10,
            Self::ConfirmSelectOk => 11,

            Self::TxSelect => 10,
            Self::TxSelectOk => 11,
            Self::TxCommit => 20,
            Self::TxCommitOk => 21,
            Self::TxRollback => 30,
            Self::TxRollbackOk => 31,
        }
    }
}

impl AmqpEncode for MethodFrame {
    fn encode(&self, buf: &mut BytesMut) {
        self.class_id().encode(buf);
        self.method_id().encode(buf);
        match self {
            Self::ConnectionStart(m) => m.encode(buf),
            Self::ConnectionStartOk(m) => m.encode(buf),
            Self::ConnectionSecure(m) => m.encode(buf),
            Self::ConnectionSecureOk(m) => m.encode(buf),
            Self::ConnectionTune(m) => m.encode(buf),
            Self::ConnectionTuneOk(m) => m.encode(buf),
            Self::ConnectionOpen(m) => m.encode(buf),
            Self::ConnectionOpenOk => {}
            Self::ConnectionClose(m) => m.encode(buf),
            Self::ConnectionCloseOk => {}
            Self::ConnectionBlocked(m) => m.encode(buf),
            Self::ConnectionUnblocked => {}

            Self::ChannelOpen => {
                // reserved short string
                super::types::ShortStr::new().encode(buf);
            }
            Self::ChannelOpenOk => {
                // reserved long string
                super::types::LongStr::new().encode(buf);
            }
            Self::ChannelFlow(m) => m.encode(buf),
            Self::ChannelFlowOk(m) => m.encode(buf),
            Self::ChannelClose(m) => m.encode(buf),
            Self::ChannelCloseOk => {}

            Self::ExchangeDeclare(m) => m.encode(buf),
            Self::ExchangeDeclareOk => {}
            Self::ExchangeDelete(m) => m.encode(buf),
            Self::ExchangeDeleteOk => {}
            Self::ExchangeBind(m) => m.encode(buf),
            Self::ExchangeBindOk => {}
            Self::ExchangeUnbind(m) => m.encode(buf),
            Self::ExchangeUnbindOk => {}

            Self::QueueDeclare(m) => m.encode(buf),
            Self::QueueDeclareOk(m) => m.encode(buf),
            Self::QueueBind(m) => m.encode(buf),
            Self::QueueBindOk => {}
            Self::QueuePurge(m) => m.encode(buf),
            Self::QueuePurgeOk(m) => m.encode(buf),
            Self::QueueDelete(m) => m.encode(buf),
            Self::QueueDeleteOk(m) => m.encode(buf),
            Self::QueueUnbind(m) => m.encode(buf),
            Self::QueueUnbindOk => {}

            Self::BasicQos(m) => m.encode(buf),
            Self::BasicQosOk => {}
            Self::BasicConsume(m) => m.encode(buf),
            Self::BasicConsumeOk(m) => m.encode(buf),
            Self::BasicCancel(m) => m.encode(buf),
            Self::BasicCancelOk(m) => m.encode(buf),
            Self::BasicPublish(m) => m.encode(buf),
            Self::BasicReturn(m) => m.encode(buf),
            Self::BasicDeliver(m) => m.encode(buf),
            Self::BasicGet(m) => m.encode(buf),
            Self::BasicGetOk(m) => m.encode(buf),
            Self::BasicGetEmpty => {
                // reserved short string
                super::types::ShortStr::new().encode(buf);
            }
            Self::BasicAck(m) => m.encode(buf),
            Self::BasicReject(m) => m.encode(buf),
            Self::BasicRecoverAsync(m) => m.encode(buf),
            Self::BasicRecover(m) => m.encode(buf),
            Self::BasicRecoverOk => {}
            Self::BasicNack(m) => m.encode(buf),

            Self::ConfirmSelect(m) => m.encode(buf),
            Self::ConfirmSelectOk => {}

            Self::TxSelect => {}
            Self::TxSelectOk => {}
            Self::TxCommit => {}
            Self::TxCommitOk => {}
            Self::TxRollback => {}
            Self::TxRollbackOk => {}
        }
    }
}

impl AmqpDecode for MethodFrame {
    fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        let class_id = u16::decode(buf)?;
        let method_id = u16::decode(buf)?;
        match (class_id, method_id) {
            (10, 10) => Ok(Self::ConnectionStart(connection::Start::decode(buf)?)),
            (10, 11) => Ok(Self::ConnectionStartOk(connection::StartOk::decode(buf)?)),
            (10, 20) => Ok(Self::ConnectionSecure(connection::Secure::decode(buf)?)),
            (10, 21) => Ok(Self::ConnectionSecureOk(connection::SecureOk::decode(buf)?)),
            (10, 30) => Ok(Self::ConnectionTune(connection::Tune::decode(buf)?)),
            (10, 31) => Ok(Self::ConnectionTuneOk(connection::TuneOk::decode(buf)?)),
            (10, 40) => Ok(Self::ConnectionOpen(connection::Open::decode(buf)?)),
            (10, 41) => {
                // OpenOk has a reserved short string
                ShortStr::decode(buf)?;
                Ok(Self::ConnectionOpenOk)
            }
            (10, 50) => Ok(Self::ConnectionClose(connection::Close::decode(buf)?)),
            (10, 51) => Ok(Self::ConnectionCloseOk),
            (10, 60) => Ok(Self::ConnectionBlocked(connection::Blocked::decode(buf)?)),
            (10, 61) => Ok(Self::ConnectionUnblocked),

            (20, 10) => {
                // reserved short string
                ShortStr::decode(buf)?;
                Ok(Self::ChannelOpen)
            }
            (20, 11) => {
                // reserved long string
                LongStr::decode(buf)?;
                Ok(Self::ChannelOpenOk)
            }
            (20, 20) => Ok(Self::ChannelFlow(channel::Flow::decode(buf)?)),
            (20, 21) => Ok(Self::ChannelFlowOk(channel::FlowOk::decode(buf)?)),
            (20, 40) => Ok(Self::ChannelClose(channel::Close::decode(buf)?)),
            (20, 41) => Ok(Self::ChannelCloseOk),

            (40, 10) => Ok(Self::ExchangeDeclare(exchange::Declare::decode(buf)?)),
            (40, 11) => Ok(Self::ExchangeDeclareOk),
            (40, 20) => Ok(Self::ExchangeDelete(exchange::Delete::decode(buf)?)),
            (40, 21) => Ok(Self::ExchangeDeleteOk),
            (40, 30) => Ok(Self::ExchangeBind(exchange::Bind::decode(buf)?)),
            (40, 31) => Ok(Self::ExchangeBindOk),
            (40, 40) => Ok(Self::ExchangeUnbind(exchange::Unbind::decode(buf)?)),
            (40, 51) => Ok(Self::ExchangeUnbindOk),

            (50, 10) => Ok(Self::QueueDeclare(queue::Declare::decode(buf)?)),
            (50, 11) => Ok(Self::QueueDeclareOk(queue::DeclareOk::decode(buf)?)),
            (50, 20) => Ok(Self::QueueBind(queue::Bind::decode(buf)?)),
            (50, 21) => Ok(Self::QueueBindOk),
            (50, 30) => Ok(Self::QueuePurge(queue::Purge::decode(buf)?)),
            (50, 31) => Ok(Self::QueuePurgeOk(queue::PurgeOk::decode(buf)?)),
            (50, 40) => Ok(Self::QueueDelete(queue::Delete::decode(buf)?)),
            (50, 41) => Ok(Self::QueueDeleteOk(queue::DeleteOk::decode(buf)?)),
            (50, 50) => Ok(Self::QueueUnbind(queue::Unbind::decode(buf)?)),
            (50, 51) => Ok(Self::QueueUnbindOk),

            (60, 10) => Ok(Self::BasicQos(basic::Qos::decode(buf)?)),
            (60, 11) => Ok(Self::BasicQosOk),
            (60, 20) => Ok(Self::BasicConsume(basic::Consume::decode(buf)?)),
            (60, 21) => Ok(Self::BasicConsumeOk(basic::ConsumeOk::decode(buf)?)),
            (60, 30) => Ok(Self::BasicCancel(basic::Cancel::decode(buf)?)),
            (60, 31) => Ok(Self::BasicCancelOk(basic::CancelOk::decode(buf)?)),
            (60, 40) => Ok(Self::BasicPublish(basic::Publish::decode(buf)?)),
            (60, 50) => Ok(Self::BasicReturn(basic::Return::decode(buf)?)),
            (60, 60) => Ok(Self::BasicDeliver(basic::Deliver::decode(buf)?)),
            (60, 70) => Ok(Self::BasicGet(basic::Get::decode(buf)?)),
            (60, 71) => Ok(Self::BasicGetOk(basic::GetOk::decode(buf)?)),
            (60, 72) => {
                // reserved short string
                ShortStr::decode(buf)?;
                Ok(Self::BasicGetEmpty)
            }
            (60, 80) => Ok(Self::BasicAck(basic::Ack::decode(buf)?)),
            (60, 90) => Ok(Self::BasicReject(basic::Reject::decode(buf)?)),
            (60, 100) => Ok(Self::BasicRecoverAsync(basic::RecoverAsync::decode(buf)?)),
            (60, 110) => Ok(Self::BasicRecover(basic::Recover::decode(buf)?)),
            (60, 111) => Ok(Self::BasicRecoverOk),
            (60, 120) => Ok(Self::BasicNack(basic::Nack::decode(buf)?)),

            (85, 10) => Ok(Self::ConfirmSelect(confirm::Select::decode(buf)?)),
            (85, 11) => Ok(Self::ConfirmSelectOk),

            (90, 10) => Ok(Self::TxSelect),
            (90, 11) => Ok(Self::TxSelectOk),
            (90, 20) => Ok(Self::TxCommit),
            (90, 21) => Ok(Self::TxCommitOk),
            (90, 30) => Ok(Self::TxRollback),
            (90, 31) => Ok(Self::TxRollbackOk),

            _ => Err(ProtocolError::UnknownMethod {
                class_id,
                method_id,
            }),
        }
    }
}

// Re-import ShortStr and LongStr for use in decode
use super::types::{LongStr, ShortStr};
