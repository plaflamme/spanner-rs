pub mod proto;

pub trait SpannerResource {
    fn id(&self) -> String;
    fn url_path(&self) -> String {
        format!("/v1/{}", self.id())
    }
    fn name(&self) -> &str;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceId {
    project: String,
    name: String,
}

impl InstanceId {
    pub fn new(project: &str, name: &str) -> Self {
        Self {
            project: project.to_string(),
            name: name.to_string(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }
}

impl SpannerResource for InstanceId {
    fn id(&self) -> String {
        format!("projects/{}/instances/{}", self.project, self.name)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseId(InstanceId, String);
impl DatabaseId {
    pub fn new(instance: &InstanceId, name: &str) -> Self {
        Self(instance.clone(), name.to_string())
    }
}

impl SpannerResource for DatabaseId {
    fn id(&self) -> String {
        format!("{}/databases/{}", self.0.id(), self.1)
    }

    fn name(&self) -> &str {
        &self.1
    }
}
