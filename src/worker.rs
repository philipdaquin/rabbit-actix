// AIM::<>  The worker will consume all the messages from the requests queue and try to decode them as QR images to strings 

use actix::System;
use failure::{format_err, Error};
use image::GenericImageView;
use log::debug;
use queens_rock::Scanner;
use rabbit_actix::queue_actor::{QueueActor, QueueHandler, TaskId};
use rabbit_actix::{QrRequest, QrResponse, REQUEST, RESPONSE};

//  Handler - this will only transform incoming messages:
struct WorkerHandler {}
//  we use the WorkerHandler struct as the handler for the queue and use it with QueueActor later

impl QueueHandler for WorkerHandler { 
    type Incoming =  QrRequest;
    type Outgoing = QrResponse;

    fn incoming(&self) -> &str { REQUEST }
    //  incoming method returns the value of the REQUESTS constant that we wil use as a name for incoming queues   
    fn outgoing(&self) -> &str { RESPONSE }
    //  outgoing method returns the RESPONSES constant, which is used as the name for the queue of outgoing messages
    fn handle(&self, _: &TaskId, incoming: Self::Incoming) -> Result<Option<Self::Outgoing>, Error> { 
        //  Handle method:  the handle method of QueueHandler takes a request and calls the scan method with data, then it converts the Result into a QrResponse and return its

        debug!("In : {:?}", incoming);

        let outgoing = self.scan(&incoming.image).into();

        debug!("Out: {:?}", outgoing);

        Ok(Some(outgoing))
    }
}

impl WorkerHandler { 
    fn scan(&self, data: &[u8]) -> Result<String, Error> { 
        let image = image::load_from_memory(data)?;
        
        let luma = image.to_luma().into_vec();
        //  Load an image from the provided bytes, converts the Image to grayscale with the to_luma method, and provides the returned value as an argument for a Scanner 
        let scanner = Scanner::new(
            luma.as_ref(),
            image.width() as usize,
            image.height() as usize,
        );
        
        scanner.scan().extract(0)

        //  scan method decodes the QR code and extracts the first code converted to a String

            .ok_or_else(|| format_err!("Can't Extract QR code"))
            
            .and_then(|code| code.decode()
                .map_err(|_| format_err!("Can't decode this image!")))
            
                .and_then(|code| {
                 code.try_string().map_err(|_| format_err!("Cannot Convert to a String!"))
            })
    }
}
fn main() -> Result<(), Error> { 
    env_logger::init();
    let mut sys = System::new("rabbit-actix-worker!");

    let _ = QueueActor::new(WorkerHandler {}, &mut sys)?;
    let _ = sys.run();

    Ok(())
}
