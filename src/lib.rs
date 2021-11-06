pub mod queue_actor;

use actix::{Message, SystemRunner};
use failure::Error;
use futures::Future;
use lapin::channel::{Channel, QueueDeclareOptions};
//  Channel: Channel type will be created as a result of a connection and is returned by a CLient
//  QueueDeclareOptions: is used as a paramter for the quque_declare method call of Channel  
use lapin::client::{Client, ConnectionOptions};
//  CLient: represents a client that conects to RabbiMQ, 
//  ConnectionOptions: is necessary to establish a connection, but we will use default values
use lapin::error::Error as LapinError;
use lapin::queue::Queue;
//  Queue: represents a queue in RabbitMQ
use lapin::types::FieldTable;
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;


//  Queue1: Request
pub const REQUEST: &str = "request";
//  Queue2: Response
pub const RESPONSE: &str = "response";

// Spawning a Client 
pub fn spawn_client(sys: &mut SystemRunner) -> Result<Channel<TcpStream>, Error> { 
    let addr = "127.0.0.1:5672".parse().unwrap();

    let fut = TcpStream::connect(&addr)
    //  Create a TcpStream from a constant address with the connect method call
    //  Connect method: returns a Future that we use to create a combinator that maps to a new Client connection to RabbitMQ
        .map_err(Error::from)
        .and_then(|stream| { 
            let options = ConnectionOptions::default();
            Client::connect(stream, options).from_err::<Error>()
        });
    let (client, heartbeat) = sys.block_on(fut)?;
    //  We use block_on of SystemRunner to execute that Future Immediately
    //  it returns a Client and Heartbeat instance
    //  Client: is used to create abn instance of Chnnake 
    //  HeartBeat: is a task that pings RabbitMQ with a connection that has to be spawned as a concurrent acitvity in the event loop
    actix::spawn(heartbeat.map_err(drop));
    //  We use spawn method to run it heartbeat because we dont;t have the Context of an Actor 

    let channel = sys.block_on(client.create_channel())?;
    //  We call the create_channel method of a Client to create a Channel 
    //  this method returns a Future, which we also execute with the block_on method 

    Ok(channel)
    //  Return Channel and implement ensure_queue method 
}
//  Ensure_queue method creates the option to call the queue_declare method, which creates a queue inside RabbitMQ
pub fn ensure_queue(chan: &Channel<TcpStream>, name: &str) -> impl Future<Item = Queue, Error = LapinError>  {
    let opts = QueueDeclareOptions { 
        auto_delete: true,
        ..Default::default()
        //  We fill QueueDeclareOptions with default parameters, but set the autodelete to be true because we want the created queue to be delted when an application ends 
    };
    let table = FieldTable::new();
    chan.queue_declare(name, opts, table)
}

//  Request and Response 
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QrRequest { 
    pub image: Vec<u8>
}
impl Message for QrRequest { 
    type Result = ();
}
//  The response type is represented by the QrResponse 
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum  QrResponse { 
    Succeed(String),
    Failed(String),
}
//  Overriding the serialization behaviour when we need it 
impl From<Result<String, Error>> for QrResponse { 
    fn from(res: Result<String, Error>) -> Self { 
        match res { 
            Ok(data) => QrResponse::Succeed(data),
            Err(err) => QrResponse::Failed(err.to_string())
        }
    }
}
impl Message for QrResponse { 
    type Result = ();
}