#[derive(Debug)]
struct Machine {
    logs: Vec<Log>,
    prev_log_index: u32,
}

#[derive(Clone, Copy, Debug)]
struct Log {
  index: u32,
  term: u32,
}

fn main() {
  let machine  = Machine{logs: Vec::new(), prev_log_index: 0};
  let ask_term: u32 = 90;
  let local_log = machine.get_log_at(32);
  match local_log {
    Some(log) => {
      if log.term != ask_term {
        println!("send outOfSync: {:?}, ask_term:{}, ask_idx:{}", machine, ask_term, 32);
        return;
      }

      println!("requirements met:: if payload is empty, heartbeat, otherwise apply logs by checking leader commit")
    },
    None => {
        println!("send outOfSync: {:?}, ask_term:{}, ask_idx:{}", machine, ask_term, 32);
    }
  }
}

impl Machine {
    fn get_log_at(&self, index: usize) -> Option<Log> {
        if self.logs.len() <= index {
            return None;
        }

        Some(self.logs[index])
    }
}
