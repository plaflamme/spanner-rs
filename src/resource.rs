pub trait SpannerResource {
    fn resources_id(&self) -> String;
    fn name(&self) -> &str;
    fn id(&self) -> String {
        format!("{}/{}", self.resources_id(), self.name())
    }
    fn url_path(&self) -> String {
        format!("/v1/{}", self.id())
    }
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
    fn name(&self) -> &str {
        &self.name
    }

    fn resources_id(&self) -> String {
        format!("projects/{}/instances", self.project)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseId(InstanceId, String);
impl DatabaseId {
    pub fn new(instance: InstanceId, name: &str) -> Self {
        Self(instance, name.to_string())
    }
}

impl SpannerResource for DatabaseId {
    fn name(&self) -> &str {
        &self.1
    }

    fn resources_id(&self) -> String {
        format!("{}/databases", self.0.id())
    }
}

#[cfg(test)]
mod test {
    use super::{DatabaseId, InstanceId, SpannerResource};

    #[test]
    fn test_instance_id() {
        let instance_id = InstanceId::new("test-project", "test-instance");
        assert_eq!(instance_id.name(), "test-instance");
        assert_eq!(
            instance_id.resources_id(),
            "projects/test-project/instances"
        );
        assert_eq!(
            instance_id.id(),
            "projects/test-project/instances/test-instance"
        );
        assert_eq!(
            instance_id.url_path(),
            "/v1/projects/test-project/instances/test-instance"
        );
    }

    #[test]
    fn test_database_id() {
        let database_id = DatabaseId::new(
            InstanceId::new("test-project", "test-instance"),
            "test-database",
        );
        assert_eq!(database_id.name(), "test-database");
        assert_eq!(
            database_id.resources_id(),
            "projects/test-project/instances/test-instance/databases"
        );
        assert_eq!(
            database_id.id(),
            "projects/test-project/instances/test-instance/databases/test-database"
        );
        assert_eq!(
            database_id.url_path(),
            "/v1/projects/test-project/instances/test-instance/databases/test-database"
        );
    }
}
