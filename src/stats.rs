use std::io::Write;

use chrono::{Duration, DurationRound};

pub fn printmissing(db: &crate::database::Db) -> anyhow::Result<()> {
    if let (Some(firstid),Some(lastid)) = db.get_first_last_id()? {
        let mut missing_ids = 0;
        let mut active_oclock : Option<chrono::DateTime<chrono::FixedOffset>> = None;
        struct Info {
            samples: u32,
            minutes: u32,
        }

        let mut map : hashbrown::HashMap<String, Info> = hashbrown::HashMap::with_capacity(300);

        println!("hour,ticker,num_samples,num_minutes");
        let so = std::io::stdout();
        let so = so.lock();
        let mut so = std::io::BufWriter::with_capacity(65536, so);

        let mut report_n_clear = |ocl: Option<chrono::DateTime<chrono::FixedOffset>>,map:&mut hashbrown::HashMap<String, Info> | {
            if let Some(t) = ocl {
                for x in map.iter() {
                    if x.1.samples > 0 {
                        let _ = writeln!(
                            so,
                            "{},{},{},{}",
                            t,
                            x.0,
                            x.1.samples,
                            x.1.minutes,
                        );
                    }
                }
            }
            for x in map.iter_mut() {
                x.1.samples = 0;
                x.1.minutes = 0;
            }
        };

        for i in firstid..=lastid {
            if let Some(x) = db.get_entry_by_id(i)? {
                let e = chrono::DateTime::parse_from_rfc3339(&x.start_time)?;
                let e = e + chrono::Duration::minutes(1);
                let e = e.duration_trunc(Duration::hours(1))?;
                //let dur = (x.e - x.s) / 1000 / 60;
                
                //print!("maplen {} , Entry of {} ", map.len(), x.t);
                let entry = map.entry(x.ticker).or_insert_with(||Info {
                    samples: 0,
                    minutes: 0,
                });

                //println!("has samples {}", entry.samples);
                entry.samples += 1;
                //entry.minutes += dur as u32;

                if active_oclock != Some(e) {
                    //println!("{:?} != {:?}", active_oclock, Some(e));
                    report_n_clear(active_oclock, &mut map);
                    active_oclock = Some(e);
                }
            } else {
                missing_ids += 1;
            }
        }
        report_n_clear(active_oclock, &mut map);
        if missing_ids > 0 {
            eprintln!("There were {} missing IDs", missing_ids);
        }
    } else {
        eprintln!("No data in this database");
    }
    Ok(())
}
