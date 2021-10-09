/// A trait for identifiable resources within Cloud Spanner.
///
/// The format is typically something like `<kind>/<name>` where `kind` is the plural form
/// of the resource kind, e.g.: `projects` or `databases` and `name` is the name of a particular instance of that resource.
///
/// For example, the database resource named `my-database` in the `my-instance` instance in the `my-gcp-project` project
/// will have the following identifier: `projects/my-gcp-project/instances/my-instance/databases/my-database`.
pub trait SpannerResource {
    /// The name of this particular instance of the resource.
    fn name(&self) -> &str;

    /// The full path to all resources under the same parent.
    ///
    /// For example, for the `InstanceId` resource, this would return something like `projects/my-project/instances`
    fn resources_path(&self) -> String;

    /// The full path to this particular resource.
    ///
    /// For example, `projects/my-project/instances/my-instance`
    fn id(&self) -> String {
        format!("{}/{}", self.resources_path(), self.name())
    }
}

/// The resource that identifies a particular GCP project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectId(String);

impl ProjectId {
    /// Creates a new `ProjectId` resource using the specified name.
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }
}

impl SpannerResource for ProjectId {
    fn name(&self) -> &str {
        &self.0
    }

    fn resources_path(&self) -> String {
        "projects".to_string()
    }
}

/// The resource that identifies a Cloud Spanner instance in a particular GCP project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceId(ProjectId, String);

impl InstanceId {
    /// Creates a new `InstanceId` resource using the specified project resource and name.
    pub fn new(project: ProjectId, name: &str) -> Self {
        Self(project, name.to_string())
    }

    /// Returns a reference to the project hosting this Cloud Spanner instance.
    pub fn project(&self) -> &ProjectId {
        &self.0
    }
}

impl SpannerResource for InstanceId {
    fn name(&self) -> &str {
        &self.1
    }

    fn resources_path(&self) -> String {
        format!("{}/instances", self.0.id())
    }
}

/// The resource that identifies a Cloud Spanner database in a particular Cloud Spanner instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseId(InstanceId, String);
impl DatabaseId {
    /// Creates a new `DatabaseId` resource using the specified instance resource and name.
    pub fn new(instance: InstanceId, name: &str) -> Self {
        Self(instance, name.to_string())
    }
}

impl SpannerResource for DatabaseId {
    fn name(&self) -> &str {
        &self.1
    }

    fn resources_path(&self) -> String {
        format!("{}/databases", self.0.id())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_project_id() {
        let project_id = ProjectId::new("test-project");
        assert_eq!(project_id.name(), "test-project");
        assert_eq!(project_id.resources_path(), "projects".to_string());
        assert_eq!(project_id.id(), "projects/test-project".to_string());
    }
    #[test]
    fn test_instance_id() {
        let instance_id = InstanceId::new(ProjectId::new("test-project"), "test-instance");
        assert_eq!(instance_id.name(), "test-instance");
        assert_eq!(
            instance_id.resources_path(),
            "projects/test-project/instances"
        );
        assert_eq!(
            instance_id.id(),
            "projects/test-project/instances/test-instance"
        );
    }

    #[test]
    fn test_database_id() {
        let database_id = DatabaseId::new(
            InstanceId::new(ProjectId::new("test-project"), "test-instance"),
            "test-database",
        );
        assert_eq!(database_id.name(), "test-database");
        assert_eq!(
            database_id.resources_path(),
            "projects/test-project/instances/test-instance/databases"
        );
        assert_eq!(
            database_id.id(),
            "projects/test-project/instances/test-instance/databases/test-database"
        );
    }
}
