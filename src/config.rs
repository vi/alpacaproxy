#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone, Debug)]
pub struct ClientConfig {
    pub uri: Option<url::Url>,

    pub startup_messages: Vec<serde_json::Value>,

    pub require_password: Option<String>,

    pub automirror: bool,
}

pub fn read_config(config_file: &std::path::Path) -> anyhow::Result<ClientConfig> {
    Ok(serde_json::from_reader(std::io::BufReader::new(
        std::fs::File::open(config_file)?,
    ))?)
}

pub fn write_config(config_file: &std::path::Path, config: &ClientConfig) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(
        std::io::BufWriter::new(std::fs::File::create(config_file)?),
        config,
    )?;
    Ok(())
}
