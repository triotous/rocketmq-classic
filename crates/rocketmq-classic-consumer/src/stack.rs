use std::{backtrace::Backtrace, thread};

pub(crate) fn dump_threads() -> String {
    #[cfg(all(
        target_os = "macos",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    {
        if let Ok(dump) = apple::dump() {
            return dump;
        }
    }

    current_thread_dump()
}

fn current_thread_dump() -> String {
    let current = thread::current();
    let name = current.name().unwrap_or("unnamed");
    format!(
        "\"{name}\" tid={:?} (current thread only on this target)\n{:?}",
        current.id(),
        Backtrace::force_capture()
    )
}

#[cfg(all(
    target_os = "macos",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
mod apple {
    use std::{
        ffi::{CStr, c_void},
        fmt::Write as _,
        mem::size_of,
        ptr, slice,
    };

    use mach2::{
        kern_return::KERN_SUCCESS,
        mach_port::mach_port_deallocate,
        mach_types::{thread_act_array_t, thread_act_t},
        message::mach_msg_type_number_t,
        task::task_threads,
        thread_act::{thread_get_state, thread_resume, thread_suspend},
        traps::mach_task_self,
        vm::{mach_vm_deallocate, mach_vm_read_overwrite},
        vm_types::{mach_vm_size_t, natural_t},
    };

    #[cfg(target_arch = "aarch64")]
    use mach2::{structs::arm_thread_state64_t, thread_status::ARM_THREAD_STATE64};
    #[cfg(target_arch = "x86_64")]
    use mach2::{structs::x86_thread_state64_t, thread_status::x86_THREAD_STATE64};

    const MAX_FRAMES: usize = 128;

    struct RawStack {
        frames: [usize; MAX_FRAMES],
        len: usize,
    }

    impl RawStack {
        fn empty() -> Self {
            Self {
                frames: [0; MAX_FRAMES],
                len: 0,
            }
        }

        fn push(&mut self, address: u64) {
            if address != 0 && self.len < self.frames.len() {
                self.frames[self.len] = address as usize;
                self.len += 1;
            }
        }

        fn into_vec(self) -> Vec<usize> {
            self.frames[..self.len].to_vec()
        }
    }

    struct ThreadList {
        task: mach2::mach_types::task_t,
        ports: thread_act_array_t,
        count: mach_msg_type_number_t,
    }

    impl Drop for ThreadList {
        fn drop(&mut self) {
            if self.ports.is_null() {
                return;
            }
            unsafe {
                for port in slice::from_raw_parts(self.ports, self.count as usize) {
                    let _ = mach_port_deallocate(self.task, *port);
                }
                let size = (self.count as usize).saturating_mul(size_of::<thread_act_t>());
                let _ = mach_vm_deallocate(self.task, self.ports as u64, size as mach_vm_size_t);
            }
        }
    }

    struct ResumeThread(thread_act_t);

    impl Drop for ResumeThread {
        fn drop(&mut self) {
            unsafe {
                let _ = thread_resume(self.0);
            }
        }
    }

    struct ThreadDump {
        name: String,
        id: u64,
        current_backtrace: Option<String>,
        frames: Vec<usize>,
    }

    pub(super) fn dump() -> Result<String, &'static str> {
        let task = unsafe { mach_task_self() };
        let mut ports = ptr::null_mut();
        let mut count = 0;
        if unsafe { task_threads(task, &mut ports, &mut count) } != KERN_SUCCESS {
            return Err("task_threads failed");
        }
        let thread_list = ThreadList { task, ports, count };
        if thread_list.ports.is_null() {
            return Err("task_threads returned no thread list");
        }

        let current_port = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
        let mut dumps = Vec::with_capacity(thread_list.count as usize);
        for &port in unsafe { slice::from_raw_parts(thread_list.ports, thread_list.count as usize) }
        {
            let (name, id) = thread_name(port);
            if port == current_port {
                dumps.push(ThreadDump {
                    name,
                    id,
                    current_backtrace: Some(format!(
                        "{:?}",
                        std::backtrace::Backtrace::force_capture()
                    )),
                    frames: Vec::new(),
                });
                continue;
            }

            if unsafe { thread_suspend(port) } != KERN_SUCCESS {
                dumps.push(ThreadDump {
                    name,
                    id,
                    current_backtrace: None,
                    frames: Vec::new(),
                });
                continue;
            }
            let resume = ResumeThread(port);
            let raw_stack = unsafe { capture_frames(task, port) };
            drop(resume);
            let frames = raw_stack.into_vec();
            dumps.push(ThreadDump {
                name,
                id,
                current_backtrace: None,
                frames,
            });
        }

        let mut output = String::new();
        for dump in dumps {
            let _ = writeln!(output, "\"{}\" tid={}", dump.name, dump.id);
            if let Some(backtrace) = dump.current_backtrace {
                for line in backtrace.lines() {
                    let _ = writeln!(output, "    {line}");
                }
            } else if dump.frames.is_empty() {
                let _ = writeln!(output, "    <native stack unavailable>");
            } else {
                for symbol in symbolize(&dump.frames) {
                    let _ = writeln!(output, "    {symbol}");
                }
            }
        }
        Ok(output)
    }

    fn thread_name(port: thread_act_t) -> (String, u64) {
        let pthread = unsafe { libc::pthread_from_mach_thread_np(port) };
        if pthread == 0 {
            return (format!("thread-{port}"), u64::from(port));
        }

        let mut id = u64::from(port);
        unsafe {
            let _ = libc::pthread_threadid_np(pthread, &mut id);
        }
        let mut name = [0i8; 64];
        if unsafe { libc::pthread_getname_np(pthread, name.as_mut_ptr(), name.len()) } == 0 {
            let value = unsafe { CStr::from_ptr(name.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            if !value.is_empty() {
                return (value, id);
            }
        }
        (format!("thread-{id}"), id)
    }

    #[cfg(target_arch = "aarch64")]
    unsafe fn capture_frames(task: mach2::mach_types::task_t, port: thread_act_t) -> RawStack {
        let mut state = arm_thread_state64_t::new();
        let mut count = arm_thread_state64_t::count();
        let result = unsafe {
            thread_get_state(
                port,
                ARM_THREAD_STATE64,
                (&mut state as *mut arm_thread_state64_t).cast::<natural_t>(),
                &mut count,
            )
        };
        if result != KERN_SUCCESS {
            return RawStack::empty();
        }
        unsafe { walk_frames(task, state.__fp, state.__pc) }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn capture_frames(task: mach2::mach_types::task_t, port: thread_act_t) -> RawStack {
        let mut state = x86_thread_state64_t::new();
        let mut count = x86_thread_state64_t::count();
        let result = unsafe {
            thread_get_state(
                port,
                x86_THREAD_STATE64,
                (&mut state as *mut x86_thread_state64_t).cast::<natural_t>(),
                &mut count,
            )
        };
        if result != KERN_SUCCESS {
            return RawStack::empty();
        }
        unsafe { walk_frames(task, state.__rbp, state.__rip) }
    }

    unsafe fn walk_frames(
        task: mach2::mach_types::task_t,
        mut frame_pointer: u64,
        program_counter: u64,
    ) -> RawStack {
        let mut output = RawStack::empty();
        output.push(program_counter);
        while frame_pointer != 0 && output.len < MAX_FRAMES {
            let mut record = [0u64; 2];
            let mut copied = 0 as mach_vm_size_t;
            let result = unsafe {
                mach_vm_read_overwrite(
                    task,
                    frame_pointer,
                    size_of::<[u64; 2]>() as mach_vm_size_t,
                    record.as_mut_ptr() as u64,
                    &mut copied,
                )
            };
            if result != KERN_SUCCESS || copied != size_of::<[u64; 2]>() as mach_vm_size_t {
                break;
            }
            output.push(record[1]);
            let next_frame_pointer = record[0];
            if next_frame_pointer <= frame_pointer
                || next_frame_pointer.saturating_sub(frame_pointer) > 1024 * 1024
            {
                break;
            }
            frame_pointer = next_frame_pointer;
        }
        output
    }

    fn symbolize(frames: &[usize]) -> Vec<String> {
        let addresses = frames
            .iter()
            .map(|address| *address as *mut c_void)
            .collect::<Vec<_>>();
        let symbols =
            unsafe { libc::backtrace_symbols(addresses.as_ptr(), addresses.len() as libc::c_int) };
        if symbols.is_null() {
            return frames
                .iter()
                .map(|address| format!("0x{address:x}"))
                .collect();
        }
        let output = unsafe { slice::from_raw_parts(symbols, frames.len()) }
            .iter()
            .map(|symbol| {
                unsafe { CStr::from_ptr(*symbol) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        unsafe { libc::free(symbols.cast()) };
        output
    }
}

#[cfg(test)]
mod tests {
    use super::dump_threads;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };

    #[test]
    fn jstack_includes_current_and_other_named_threads() {
        let (started, ready) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let worker_release = release.clone();
        let worker = std::thread::Builder::new()
            .name("running-info-stack-worker".to_owned())
            .spawn(move || {
                started.send(()).unwrap();
                while !worker_release.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            })
            .unwrap();
        ready.recv().unwrap();

        let dump = dump_threads();
        release.store(true, Ordering::Release);
        worker.join().unwrap();

        assert!(dump.contains("running-info-stack-worker"), "{dump}");
        assert!(dump.contains("tid="), "{dump}");
        assert!(dump.contains("Backtrace"), "{dump}");
        let lines = dump.lines().collect::<Vec<_>>();
        let worker_line = lines
            .iter()
            .position(|line| line.contains("running-info-stack-worker"))
            .unwrap();
        assert!(
            lines
                .get(worker_line + 1)
                .is_some_and(|line| !line.contains("native stack unavailable")),
            "{dump}"
        );
    }
}
