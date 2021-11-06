//  AIM:: ABstract handler to process incoming messages and can send new messages to a queue
//  This also creates all the necessary queues in RabbitMQ and subscribe to new events in correspondinbg queue
////////////////////////////////////////////

use super::{ensure_queue, spawn_client}; // ensure_queue creates a new queue, spawn_client lets us 
//  create a new CLient connected to a message broker
use actix::fut::wrap_future;
//  wrap_future converts any Future objec into an ActorFuture, which can be spawned in the Context environent of the Actix framework 
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, StreamHandler, SystemRunner};
use failure::{format_err, Error};
use futures::Future;
use lapin::channel::{BasicConsumeOptions, BasicProperties, BasicPublishOptions, Channel};
//  -Channel - represents a connection channel with the RabbitMQ insntance 
//  -BasicConsumeOptions represents options used or the basic_consume method of the Channel call to subscribe to new events in aqueue
//  -Basic_properties types are used as parameters for the basic_publish method call of the Channel type for add properties such as correlations IDs to disnct recipients of the message or set 
//  >> or set the requred quality level for delivery of messages
//  -BasiccPublishOptions is used for thte basic_publish call to set extra options for a message publishing activity

use lapin::error::Error as LapinError;

use lapin::message::Delivery;
//  Delivery represents an incoming message delivered from a queue 
use lapin::types::{FieldTable, ShortString};
// -FieldTable is used as a parameter for the basic_consume method call of the Channel type
//  -ShortString is a simple alias to a String that is used as a name of a queue in the lapin crate 
use log::{debug, warn};
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use uuid::Uuid;
//  -UUID is imported frim the uuid crate genereate unique correlations IDs for messages to identify the origin of a message 

pub type TaskId = ShortString;
//  ...the abstract handler for our handler

//  Abstract Messages handler 
pub trait QueueHandler: 'static { 
    //  QueueHandler is a trait that has two associated types and three methods 
    // --Requires a static lifetime because instances of this trait will be used as fields of actors that have a static lifetime as well 
    
    type Incoming: for<'de> Deserialize <'de>;
    //  Incoming, which represents the incoming message type and reques the type to implement the Deserialise trait to be deserializable, 
    //   >> because RabbitMQ transfer byte arrays only and you have to decide whihc format to use for serailisation 
    type Outgoing: Serialize;
    //  Outgoing, implemenets the Serialize trait to bve serializable as a byts array to be sent as an outgoing message

    // Methods in QueueHandler: 
    fn incoming(&self) -> &str;
    // Incoming - returns the name of a queue to consume incoming messages
    fn outgoing(&self) -> &str;
    //  Outgoing - returns the name of a queue in which an actor will write sending messages 

    fn handle( &self, id: &TaskId, incoming: Self::Incoming) -> Result<Option<Self::Outgoing>, Error>;
    //  Handle, which takes a reference to TaskId and incomign messages of the Self::Incoming  associated type and
    //  If the implementation returns None then no messages will be sent ti the outoging chnnale
}

// Actor 
//  This actor implements the QueueHandler trait
pub struct QueueActor<T: QueueHandler> { 
    channel: Channel<TcpStream>,
    //  This is the connection channel to Rabbit MQ, We build it over TcpStream  
    handler: T,
    //  This handler implements QueueHandler
}
//  This struct has to implmenet the Actor trait to become an Actor
impl<T: QueueHandler> Actor for QueueActor<T> { 
    type Context = Context<Self>;
    //  We added a started metjod 
    fn started(&mut self, _: &mut Self::Context) { }
}
//  Initialising all the queue
impl<T: QueueHandler> QueueActor<T> {
    pub fn new(handler: T, mut sys: &mut SystemRunner) -> Result<Addr<Self>, Error> { 
        let channel = spawn_client(&mut sys)?;
        //  Call the spawn_client function to create a Client that's connected  to message broker 
        let chan = channel.clone();
        let fut = ensure_queue(&chan, handler.outgoing());
        //  We use Channel to ensure the queue we need exists, or create it with ensure_queue
        //  We use QUeueHanadler::outgoing method to get the name of the queue to create 

        sys.block_on(fut)?;
        //  This method expects SystemRunner to execute Future objects immediiately by calling the block_on method 
        //  This lets use get a Result and interrupts other acitivites if the method call fails

        let fut = ensure_queue(&chan, handler.incoming()).and_then(move |queue| { 
            //  Create a queue using the name we get with the QueueHandler::incoming method call 
            
            let opts = BasicConsumeOptions { ..Default::default()};
            //  WE consumes messages from this queue and use the basic_consume method of a Channel that start lsitening for new messages 
            //  To call basic_consume, we created default values of the BasicConsumeOptions and Fieldtable types 

            let table = FieldTable::new();
            
            let name = format!("{}-consumer", queue.name());
            chan.basic_consume(&queue, &name, opts, table)
            //  basic_consume returns a Future that wil be resolved to be a Stream vale
        });
        let stream = sys.block_on(fut)?;
        //  We use the block_on method call of the SystemRunenr isntance ot eecut rehtis Future to get a Stream Insntance to attach it to QueueActor
        let addr = QueueActor::create(move |ctx| { 
            //  We create a QueueActor instance using the create method call, which expects a closure which inturn takes a reference to a context
            ctx.add_stream(stream);
            Self { 
                channel,
                handler
            }
        });
        Ok(addr)
    }
}
//  Handling an incoming stream
//  SInce we want to attach that Stream to the actor, wehave to implement StreamHandler for the QueueActor type
impl<T: QueueHandler> StreamHandler<Delivery, LapinError> for QueueActor<T> { 
    //  StreamHandler implementation expects a Delivery instnance 
    fn handle(&mut self, item: Delivery, ctx: &mut Context<Self>) { 
        debug!("Message received!");
       
        let fut = self.channel.basic_ack(item.delivery_tag, false).map_err(drop);
        //  RabbitMq expects that a client will send an acknowledgement when it consuems the develiered message 
        //  We do it with basic_ack emthid call of a Channel instance storeed as a field of QueueActor 
        //  THis method returns a Future instance that we will spawn in a Context to send an acknowledgemnt that the message was recieved
        ctx.spawn(wrap_future(fut));
        match self.process_message(item, ctx) { 
            //  We use the process_message method -> this will process a message using the QueueHandler isntance  
            Ok(pair) => { 
                //  Send outgoing queue using the send_message method 
                if let Some((corr_id, data)) = pair { 
                    self.send_message(corr_id, data, ctx);
                    // Add a message to initiate an outgoing message 
                }
            }
            Err(err) => { 
                warn!("Message processing error: {}", err);
            }
        }
    }
}
//  Sending a new message 
//  This actor will send task to a worker 
pub struct SendMessage<T>(pub T);

impl<T> Message for SendMessage<T> { 
    type Result = TaskId;
    //  WE need to set the Result type to TaskID, because we will genereagte a new task Id for a new message to process the response with a handler later
}
impl<T: QueueHandler> Handler<SendMessage<T::Outgoing>> for QueueActor<T> { 
    //  The Handler of this message type will generate a new UUID and convert it into a String. Then the method will use the send_message enethod to send a message to an outgoing queue
    type Result = TaskId;
    
    fn handle(&mut self, msg: SendMessage<T::Outgoing>, ctx: &mut Self::Context) -> Self::Result { 
        let corr_id = Uuid::new_v4().to_simple().to_string();
        self.send_message(corr_id.clone(), msg.0, ctx);
        corr_id
    }
}
//  Utility Methods: Process_message and send_message
impl<T: QueueHandler> QueueActor<T> { 
    fn process_message(&self,
         item: Delivery,
        _: &mut Context<Self>, ) -> Result<Option<(ShortString, T::Outgoing)>, Error> { 
       
        let corr_id = item.properties.correlation_id().to_owned().ok_or_else(|| 
            format_err!("Message has no address for the response"))?;
        //  Get a unique ID associated with a message (ie. UUID)
        let incoming = serde_json::from_slice(&item.data)?;
        //  We use JSOn format for our messages and we parse us8ing serde_json to create incoming
        //  data that is stored in the data field of the Delivery instance 
        //  If a Deserialisation was successful, we take the value of teh Self::Incoming type
        let outgoing = self.handler.handle(&corr_id, incoming)?;
        //  The handler returns a Self::Outgoing message instance, but we won't serialise it
        //  immediately for sending, because it will use the send_messagge method that we used to
        //  process incoming messages 

        if let Some(outgoing) = outgoing {
            Ok(Some((corr_id, outgoing)))
        } else {
            Ok(None)
        }
    } 
    fn send_message(&self, corr_id: ShortString, outgoing: T::Outgoing, ctx: &mut Context<Self>) {
        let data = serde_json::to_vec(&outgoing);
        //  Serialise a value to binary data 
        match data {
            //  If value is serialised to JSON successfully, we prepapre options and properties to call the basic_publish method of a channel to send a message to an outgoing queue
            Ok(data) => {
                let opts = BasicPublishOptions::default();
                let props = BasicProperties::default().with_correlation_id(corr_id);

                debug!("Sending to: {}", self.handler.outgoing());

                let fut = self.channel.basic_publish("", self.handler.outgoing(), data, opts, props)
                    .map(drop)
                    .map_err(drop);
                ctx.spawn(wrap_future(fut));
                //  Publishing a message returns a Future instnace, which we have to spawn in the context of an Actor
            }
            Err(err) => {
                //  If we can't serailise a value, we will log an error 
                warn!("Can't encode an outgoing message: {}", err);
            }
        }
    } 
}
