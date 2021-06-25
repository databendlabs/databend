pub struct ExitGuard<F: Fn()> {
    function: F,
}

impl<F: Fn()> ExitGuard<F> {
    pub fn create(f: F) -> ExitGuard<F> {
        ExitGuard { function: f }
    }
}

impl<F: Fn()> Drop for ExitGuard<F> {
    fn drop(&mut self) {
        (self.function)();
    }
}
