use hashbrown::{HashMap, HashSet};



/// Generates a fresh ID using `cur_id` as a state.
/// IDs don't repeat.
fn gen_id(cur_id: &mut SliceId) -> Option<SliceId> {
    let id = *cur_id;
    id.checked_add(1).map(|new_cur_id| {
        *cur_id = new_cur_id;
        id
    })
}



/// The number of bits in a SHA-1 hash.
pub const INFO_HASH_BIT_N: u8 = 160;

/// The number of bytes in a SHA-1 hash.
pub const INFO_HASH_BYTE_N: u8 = INFO_HASH_BIT_N.div_ceil(8);

/// See [`INFO_HASH_BYTE_N`].
pub const INFO_HASH_BYTE_N_USIZE: usize = INFO_HASH_BYTE_N as usize;

pub type InfoHash = [u8; INFO_HASH_BYTE_N_USIZE];



pub type SliceId = usize;

/// Slice attributes.
#[derive(Debug)]
pub struct SliceAttr<Payload, ReadReq> {
    /// The info hash of the torrent this slice belongs to.
    info_hash: InfoHash,

    payload: Payload,

    /// Slice read request.
    /// In `Some(piece_i, _)`, `piece_i` is a piece index.
    read_req: Option<(u32, ReadReq)>,
}

impl<Payload, ReadReq> SliceAttr<Payload, ReadReq> {
    pub fn get_info_hash(&self) -> &InfoHash { &self.info_hash }
    pub fn get_payload(&self) -> &Payload { &self.payload }
    pub fn get_payload_mut(&mut self) -> &mut Payload { &mut self.payload }
}

#[derive(Debug, thiserror::Error)]
pub enum SliceDbError {
    #[error("the slice is not found")]
    NoSlice,

    #[error("the slice DB is inconsistent")]
    InconsistentDb,

    #[error("slice ID generation failed")]
    GenIdFailed,

    #[error("multiple simultaneous read requests to a slice are forbidden")]
    MultipleRead,
}

/// Slice DB. Slices are identified by [`SliceId`].
/// Every slice has attributes defined in [`SliceAttr`].
#[derive(Debug)]
pub struct SliceDb<Payload, ReadReq> {
    /// For generating slice IDs using [`gen_id`].
    cur_id: SliceId,

    attrs: HashMap<SliceId, SliceAttr<Payload, ReadReq>>,

    /// For each `info_hash`, `slice_id` is in to `torrents.get(info_hash)`
    /// iff `info_hash` equals the corresponding attribute of slice `slice_id`.
    torrents: HashMap<InfoHash, HashSet<SliceId>>,

    /// For each `info_hash` and `piece_i`,
    /// `slice_id` is in `read_reqs.get((info_hash, piece_i))` iff
    /// `info_hash` equals the corresponding attribute of slice `slice_id`
    /// and the `read_req` attribute of slice `slice_id` is `Some(piece_i, _)`.
    read_reqs: HashMap<(InfoHash, u32), HashSet<SliceId>>,

    /// Dummy. It is constant.
    empty_slice_ids: HashSet<SliceId>,
}

impl<Payload, ReadReq> Default for SliceDb<Payload, ReadReq> {
    fn default() -> Self {
        Self {
            cur_id: Default::default(),
            attrs: Default::default(),
            torrents: Default::default(),
            read_reqs: Default::default(),
            empty_slice_ids: Default::default(),
        }
    }
}

impl<Payload, ReadReq> SliceDb<Payload, ReadReq> {
    pub fn get(
        &self,
        slice_id: SliceId,
    ) -> Option<&SliceAttr<Payload, ReadReq>> {
        self.attrs.get(&slice_id)
    }

    pub fn get_mut(
        &mut self,
        slice_id: SliceId,
    ) -> Option<&mut SliceAttr<Payload, ReadReq>> {
        self.attrs.get_mut(&slice_id)
    }

    pub fn insert(
        &mut self,
        info_hash: InfoHash,
        payload: Payload,
    ) -> Result<SliceId, SliceDbError> {
        let id = gen_id(&mut self.cur_id).ok_or(SliceDbError::GenIdFailed)?;
        let attr = SliceAttr { info_hash, payload, read_req: None };
        if self.attrs.insert(id, attr).is_some() {
            return Err(SliceDbError::InconsistentDb);
        }
        if !self.torrents.entry(info_hash).or_default().insert(id) {
            return Err(SliceDbError::InconsistentDb);
        }
        Ok(id)
    }

    pub fn remove(&mut self, slice_id: SliceId) -> Result<(), SliceDbError> {
        let attr = self.attrs.remove(&slice_id).ok_or(SliceDbError::NoSlice)?;
        let info_hash = attr.info_hash;
        let mut is_db_inconsistent = false;
        match self.torrents.get_mut(&info_hash) {
            None => is_db_inconsistent = true,
            Some(a) => if !a.remove(&slice_id) { is_db_inconsistent = true; },
        }
        if let Some((piece_i, _)) = attr.read_req {
            match self.read_reqs.get_mut(&(info_hash, piece_i)) {
                None => is_db_inconsistent = true,
                Some(a) => if !a.remove(&slice_id) { is_db_inconsistent = true; },
            }
        }
        if is_db_inconsistent { Err(SliceDbError::InconsistentDb) } else { Ok(()) }
    }

    /// Inserts a read request to the slice.
    pub fn insert_read_req(
        &mut self,
        slice_id: SliceId,
        piece_i: u32,
        req: ReadReq,
    ) -> Result<(), (ReadReq, SliceDbError)> {
        match self.attrs.get_mut(&slice_id) {
            None => Err((req, SliceDbError::NoSlice)),
            Some(attr) => match attr.read_req {
                Some(_) => Err((req, SliceDbError::MultipleRead)),
                None => {
                    let info_hash = attr.info_hash;
                    if self.read_reqs.entry((info_hash, piece_i)).or_default().insert(slice_id) {
                        debug_assert!(attr.read_req.replace((piece_i, req)).is_none());
                        Ok(())
                    } else {
                        Err((req, SliceDbError::InconsistentDb))
                    }
                },
            },
        }
    }

    /// Removes and returns all read requests to `i`.
    /// In `i` of the form `(_, piece_i)`, , `piece_i` is a piece index.
    /// If it returns `Ok(a)`, elements of `a` are of the form `(_, piece_i, _)`
    /// where `piece_i` is a piece index.
    pub fn remove_read_reqs(
        &mut self,
        i: &(InfoHash, u32),
    ) -> Result<Vec<(SliceId, u32, ReadReq)>, SliceDbError> {
        let mut r: Vec<(SliceId, u32, ReadReq)> = Default::default();
        for slice_id in self.read_reqs.remove(i).unwrap_or_default() {
            let attr = self.get_mut(slice_id).ok_or(SliceDbError::InconsistentDb)?;
            let (piece_i, req) = attr.read_req.take().ok_or(SliceDbError::InconsistentDb)?;
            r.push((slice_id, piece_i, req));
        }
        Ok(r)
    }

    pub fn payloads(&self, info_hash: &InfoHash) -> impl Iterator<Item = &Payload> {
        self
            .torrents
            .get(info_hash)
            .unwrap_or(&self.empty_slice_ids)
            .iter()
            .filter_map(|slice_id| self.attrs.get(slice_id))
            .map(|attr| &attr.payload)
    }
}
