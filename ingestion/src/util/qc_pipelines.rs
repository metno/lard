use chronoutil::RelativeDuration;
use rove::pipeline::{self, derive_num_leading_trailing, Pipeline};
use serde::Deserialize;
use std::{collections::HashMap, path::Path};

#[derive(Deserialize)]
struct Header {
    param_id: i32,
    time_resolution: String,
    #[allow(dead_code)]
    sensor: Vec<i32>,
}

#[derive(Deserialize)]
struct PipelineDef {
    header: Header,
    pipeline: Pipeline,
}

pub fn load_pipelines(
    path: impl AsRef<Path>,
) -> Result<HashMap<(i32, RelativeDuration), Pipeline>, pipeline::Error> {
    std::fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;

            if !entry.file_type()?.is_file() {
                return Err(pipeline::Error::DirectoryStructure);
            }

            let mut pipeline_def: PipelineDef =
                toml::from_str(&std::fs::read_to_string(entry.path())?)?;
            (
                pipeline_def.pipeline.num_leading_required,
                pipeline_def.pipeline.num_trailing_required,
            ) = derive_num_leading_trailing(&pipeline_def.pipeline);

            let key = (
                pipeline_def.header.param_id,
                // TODO: remove unwrap
                RelativeDuration::parse_from_iso8601(&pipeline_def.header.time_resolution).unwrap(),
            );

            Ok(Some((key, pipeline_def.pipeline)))
        })
        .filter_map(Result::transpose)
        .collect()
}
