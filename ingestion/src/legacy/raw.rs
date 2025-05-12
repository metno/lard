use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

pub async fn ingest() -> Result<(), Error> {
    // TODO

    Ok(())
}
