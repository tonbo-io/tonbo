// Uses own Tonbo dummy instance on EC2 to do compaction
#[allow(unused)]
struct RemoteCompactor {}

impl RemoteCompactor {
    #[allow(unused)]
    // Initialize dummy Tonbo instance to do remote compaction
    pub fn new() -> Self {
        Self {}
    }
}
