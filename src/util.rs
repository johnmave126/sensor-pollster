use anyhow::anyhow;
use std::fmt::Display;

pub(crate) trait AttachContext<T> {
    fn attach_context<C>(self, context: C) -> anyhow::Result<T>
    where
        C: Display + Send + Sync + 'static;
    fn attach_with_context<C, F>(self, f: F) -> anyhow::Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T, E> AttachContext<T> for Result<T, E>
where
    E: std::error::Error + Into<anyhow::Error>,
{
    fn attach_context<C>(self, context: C) -> anyhow::Result<T>
    where
        C: Display + Send + Sync + 'static,
    {
        self.map_err(move |e| anyhow!(e).context(context))
    }
    fn attach_with_context<C, F>(self, f: F) -> anyhow::Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(move |e| anyhow!(e).context(f()))
    }
}
