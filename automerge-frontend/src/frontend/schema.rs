use std::collections::HashSet;

use crate::Path;

/// Schema for the frontend to build state with.
#[derive(Debug, Default, Clone)]
pub struct Schema {
    /// Any path in the frontend with this prefix will use a sorted map rather than a normal map.
    pub sorted_maps_prefixes: HashSet<Path>,
    /// Any path in the frontend with this exact path will use a sorted map rather than a normal map.
    pub sorted_maps_exact: HashSet<Path>,
}

impl Schema {
    pub(crate) fn is_sorted_map(&self, path: &Path) -> bool {
        self.sorted_maps_exact.contains(path)
            || self
                .sorted_maps_prefixes
                .iter()
                .any(|prefix| path.has_prefix(prefix))
    }
}
