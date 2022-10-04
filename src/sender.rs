use crate::Conf;

pub struct Sender {
    conf: Conf,
}

impl Sender {
    pub fn new(conf: Conf) -> Self {
        Sender { conf }
    }
}
