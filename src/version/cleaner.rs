use std::{collections::BTreeMap, fs, io, sync::Arc};

use flume::{Receiver, Sender};

use crate::{fs::FileId, DbOption};

pub(crate) enum CleanTag {
    Add {
        version_num: usize,
        gens: Vec<FileId>,
    },
    Clean {
        version_num: usize,
    },
}

pub(crate) struct Cleaner {
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<usize, (Vec<FileId>, bool)>,
    option: Arc<DbOption>,
}

impl Cleaner {
    pub(crate) fn new(option: Arc<DbOption>) -> (Self, Sender<CleanTag>) {
        let (tag_send, tag_recv) = flume::bounded(option.clean_channel_buffer);

        (
            Cleaner {
                tag_recv,
                gens_map: Default::default(),
                option,
            },
            tag_send,
        )
    }

    pub(crate) async fn listen(&mut self) -> Result<(), io::Error> {
        while let Ok(tag) = self.tag_recv.recv_async().await {
            match tag {
                CleanTag::Add { version_num, gens } => {
                    let _ = self.gens_map.insert(version_num, (gens, false));
                }
                CleanTag::Clean { version_num } => {
                    if let Some((_, dropped)) = self.gens_map.get_mut(&version_num) {
                        *dropped = true;
                    }
                    while let Some((first_version, (gens, dropped))) = self.gens_map.pop_first() {
                        if !dropped {
                            let _ = self.gens_map.insert(first_version, (gens, false));
                            continue;
                        }
                        for gen in gens {
                            fs::remove_file(self.option.table_path(&gen))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

// TODO: TestCase
