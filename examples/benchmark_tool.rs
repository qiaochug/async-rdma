// #![feature(get_mut_unchecked)]
use async_rdma::{
    LocalMr, RemoteMr,
    LocalMrReadAccess, LocalMrWriteAccess,
    // RemoteMrReadAccess, RemoteMRWriteAccess,
    Rdma, RdmaBuilder};
use clippy_utilities::Cast;
use std::{alloc::Layout, 
    env, io::{self, Write}, 
    process::exit, time::{Instant, Duration},
    sync::{Arc},
    ops::{DerefMut, Deref}};
use rand::Rng;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

async fn prepare_mrs(rdma: &Rdma, local_mrs: &mut Vec<Arc<RwLock<LocalMr>>>, remote_mrs: &mut Vec<Arc<RwLock<RemoteMr>>>,
    req_num: usize, req_size: usize, iamserver: bool) -> io::Result<()>{

    for i in 0..req_num {
        // Allocating local memory
        let mut lmr = rdma.alloc_local_mr(Layout::array::<u8>(req_size).unwrap())?;

        // write data into lmr
        let i_u8 : u8 = u8::try_from(i % 10).ok().unwrap();
        let fill: u8 = if iamserver { 10 + i_u8 } else { i_u8 };
        for byte in lmr.get_mut(0..req_size).unwrap().as_mut_slice().iter_mut() {
            *byte = fill;
        }

        if iamserver {
            // server send local mrs to client
            rdma.send_local_mr(lmr).await?;
        } else {
            // client saves local mrs
            local_mrs.push(Arc::new(RwLock::new(lmr)));

            // client receives and save remote mrs
            let rmr = rdma.receive_remote_mr().await?;
            remote_mrs.push(Arc::new(RwLock::new(rmr)));
        }
    }

    Ok(())
}

async fn return_mrs(rdma: &Rdma, local_mrs: &mut Vec<Arc<RwLock<LocalMr>>>, remote_mrs: &mut Vec<Arc<RwLock<RemoteMr>>>,
    req_num: usize, iamserver: bool) -> io::Result<()>{

    for i in 0..req_num {
        if iamserver {
            // server receives local mrs
            let lmr = rdma.receive_local_mr().await?;
            local_mrs.push(Arc::new(RwLock::new(lmr)));
        } else {
            let rmr = remote_mrs.remove(0);
            // client sends remote mrs
            rdma.send_remote_mr(Arc::into_inner(rmr).unwrap().into_inner()).await?;
        }
    }

    Ok(())
}

async fn print_mrs_wrapped(rdma: &Rdma, local_mrs: &Vec<Arc<RwLock<LocalMr>>>) -> io::Result<()>{

    for i in 0..local_mrs.len() {
        let mr_r = local_mrs[i].read();
        let slice = mr_r.as_slice();
        println!("Index {} First {:?} Last {:?} Size {}", i, slice.get(0), slice.get(slice.len() - 1), slice.len());
    }

    Ok(())

}

async fn execute_request(rdma: Arc<Rdma>, index: usize, op: usize, lmr: Arc<RwLock<LocalMr>>, rmr: Arc<RwLock<RemoteMr>>) -> io::Result<op_ret> {
    let start_t = Instant::now();    
    if op == 1 {
        rdma.read(lmr.write().deref_mut(), rmr.read().deref()).await;
    } else {
        rdma.write(lmr.read().deref(), rmr.write().deref_mut()).await;
    }
    let end_t = Instant::now();

    let result = op_ret {
        index,
        op,
        start_t,
        end_t
    };

    Ok(result)
}

#[derive(Debug)]
struct op_ret {
    index: usize,
    op: usize,
    start_t: Instant,
    end_t: Instant
}

#[derive(Debug)]
struct op_ret_adj {
    index: usize,
    op: f64,
    start_t: f64,
    end_t: f64,
    duration: f64
}

fn allocate_workload(req_num: usize, qp_num: usize) -> Vec<usize> {
    let mut sub_req_num = vec![req_num / qp_num; qp_num]; // Initialize with equal distribution
    let remainder = req_num % qp_num;

    // Distribute the remainder req_num evenly
    for i in 0..remainder {
        sub_req_num[i] += 1;
    }

    println!("Each QP assigned req_num: {:?}", sub_req_num);

    let mut allocation = Vec::with_capacity(req_num);
    for i in 0..qp_num {
        for j in 0..sub_req_num[i] {
            allocation.push(i);
        }
    }

    println!("Flattened QP assignment: {:?}", allocation);

    allocation
}

async fn run_experiment(
    req_num: usize, req_size: usize, read_pct: f64, qp_num: usize, rdmas: &Vec<Arc<Rdma>>, 
    local_mrs: &Vec<Arc<RwLock<LocalMr>>>, remote_mrs: &Vec<Arc<RwLock<RemoteMr>>>,
    avg_lat: &mut Vec<f64>, avg_thr: &mut Vec<f64>, lat_std: &mut Vec<f64>, eff_read_pct: &mut Vec<f64>
) -> io::Result<()> {

    let mut rng = rand::thread_rng();
    let mut operation_list: Vec<usize> = Vec::new();

    // Generate random 0s and 1s to represent read (1) or write (0)
    let mut read_num = 0;
    for _ in 0..req_num {
        let random_value: f64 = rng.gen();
        if random_value < read_pct {
            operation_list.push(1);
            read_num += 1;
        } else {
            operation_list.push(0);
        }
    }
    let effective_read_pct = read_num as f64/req_num as f64;
    println!("Effective read/write {:?}:", operation_list);

    // Generate QP assignments
    let mut qp_alloc: Vec<usize> = Vec::new();
    qp_alloc = allocate_workload(req_num, qp_num);

    // Create and execute requests
    let mut jhs = Vec::with_capacity(req_num);
    for i in 0..req_num {
        let rdma_qp = Arc::clone(&rdmas[qp_alloc[i]]);
        let op = operation_list[i];
        let lmr = Arc::clone(&local_mrs[i]);
        let rmr = Arc::clone(&remote_mrs[i]);
        let jh = tokio::spawn(execute_request(rdma_qp, i, op, lmr, rmr));
        jhs.push(jh);
    }

    let mut results = Vec::with_capacity(req_num);
    for i in 0..req_num {
        let jh = jhs.remove(0);
        results.push(jh.await.unwrap().unwrap());
    }

    // Result analysis
    let mut min_t = results[0].start_t;
    let mut max_t = results[0].end_t;
    for i in 1..req_num {
        if results[i].start_t < min_t {
            min_t = results[i].start_t;
        }
        if results[i].end_t > max_t {
            max_t = results[i].end_t;
        }
    }

    let mut total_lat: f64 = 0.0;
    let mut total_read: f64 = 0.0;
    let mut lats: Vec<f64> = Vec::with_capacity(req_num);
    for i in 0..req_num {
        let result = results.remove(0);
        let result_adj = op_ret_adj {
            index: result.index, 
            op : result.op as f64,
            start_t: (result.start_t - min_t).as_nanos() as f64,
            end_t: (result.end_t - min_t).as_nanos() as f64,
            duration: (result.end_t - result.start_t).as_nanos() as f64
        };
        lats.push(result_adj.duration);
        total_lat += result_adj.duration;
        total_read += result_adj.op;

        println!("{:?}", result_adj);
    }

    let avg_lat = total_lat / req_num as f64;
    let total_duration_sec = (max_t - min_t).as_secs_f64();
    let avg_thr = req_size as f64 * req_num as f64 / total_duration_sec;
    let lat_std = calc_summary_stat(&lats).std;
    let eff_read_pct = total_read / req_num as f64;
    println!("average_latency: {:.0} nsec, average_throughput {:.0} byte/sec, latency_std: {:.0}, effective_read_pct: {:.2}",
    avg_lat, avg_thr, lat_std, eff_read_pct);

    Ok(())
}

struct summary_stat {
    len: f64,
    sum: f64,
    mean: f64,
    std: f64
}

fn calc_summary_stat(data: &Vec<f64>) -> summary_stat {

    let len: f64 = data.len() as f64;
    let mut sum: f64 = 0.0;
    for i in 0..data.len() {
        sum += data[i];
    }

    let mean = sum / len;

    let mut std: f64 = 0.0;
    if data.len() > 1 {
        let mut se: f64 = 0.0;
        for i in 0..data.len() {
            let diff = data[i] - mean;
            se += diff * diff;
        } 

        std = (se / (len - 1.0)).sqrt();
    }

    return summary_stat { len, sum, mean, std };
}
#[tokio::main]
async fn main() {
    println!("Benchmark tool start");

    let args: Vec<String> = env::args().collect();
    
    let mut iamserver = false;
    let mut ip = "172.29.0.118";
    let mut port = "19875";
    let mut req_num = 8;
    let mut req_size = 4096;
    let mut qp_num = 1;
    let mut read_pct = 1.0;

    let usage = r#"Usage: cargo run --example client_configurable iamserver=<0or1> host=<server-addr>
     port=<server-port> req_num=<req_num> req_size=<req_size>
      qp_num=<qp_num> read_pct=<read_pct>"#;

    for arg in args.iter().skip(1) {
        if arg.starts_with("iamserver=") {
            iamserver = arg[10..].parse().unwrap();
        } else if arg.starts_with("host=") {
            ip = &arg[5..];
        } else if arg.starts_with("port=") {
            port = &arg[5..];
        } else if arg.starts_with("req_num=") {
            req_num = arg[8..].parse().unwrap();
        } else if arg.starts_with("req_size=") {
            req_size = arg[9..].parse().unwrap();
        } else if arg.starts_with("qp_num=") {
            qp_num = arg[7..].parse().unwrap();
        } else if arg.starts_with("read_pct=") {
            read_pct = arg[9..].parse().unwrap();
        } else {
            println!("{}", usage);
        }
    }
    let addr = format!("{}:{}", ip, port);

    println!("I am server: {}", iamserver);
    println!("Server IP: {}", ip);
    println!("Port: {}", port);
    println!("Addr: {}", addr);
    println!("Request Number: {}", req_num);
    println!("Request Size: {}", req_size);
    println!("QP Number: {}", qp_num);
    println!("Read Percentage: {}", read_pct);

    // Prepare QPs
    let rdma_builder = RdmaBuilder::default().set_dev("mlx5_0").set_imm_flag_in_wc(2).unwrap();
    let mut rdmas = Vec::with_capacity(qp_num);

    let mut rdma_o = if iamserver {
        rdma_builder.listen(addr.clone()).await.unwrap()
    } else {
        rdma_builder.connect(addr.clone()).await.unwrap()
    };
    // println!("RDMA 0 qp{:?}", rdma_o);
    for i in 1..qp_num {
        let rdma = if iamserver {
            rdma_o.listen().await.unwrap()
        } else {
            rdma_o.new_connect(addr.clone()).await.unwrap()
        };
        // println!("RDMA {} qp {:?}", i, rdma);
        let rdma_arc = Arc::new(rdma);
        rdmas.push(rdma_arc);
    }
    let rdma_arc = Arc::new(rdma_o);
    rdmas.insert(0, rdma_arc);

    let mut local_mrs = Vec::with_capacity(req_num);
    let mut remote_mrs = Vec::with_capacity(req_num);
    // only the client has access to local_mrs and remote_mrs for now
    prepare_mrs(&rdmas[0], &mut local_mrs, &mut remote_mrs, req_num, req_size, iamserver).await.unwrap();
    println!("-------- Setup MRs: --------");
    println!("self-accessible local_mrs num: {}", local_mrs.len());
    println!("self-accessible remote_mrs num: {}", remote_mrs.len());
    print_mrs_wrapped(&rdmas[0], &local_mrs).await;

    if iamserver {
        println!("Server up!");
    } else {
        println!("Client up!");
    }

    let EXPERIMENT_REPEATS = 3;
    let mut avg_lat = Vec::with_capacity(EXPERIMENT_REPEATS);
    let mut avg_thr = Vec::with_capacity(EXPERIMENT_REPEATS);
    let mut lat_std = Vec::with_capacity(EXPERIMENT_REPEATS);
    let mut eff_read_pct = Vec::with_capacity(EXPERIMENT_REPEATS);
    if !iamserver {
        for i in 0..EXPERIMENT_REPEATS {
            run_experiment(req_num, req_size, read_pct, qp_num,
                &rdmas, &local_mrs, &remote_mrs,
                &mut avg_lat, &mut avg_thr, &mut lat_std, &mut eff_read_pct).await;
        }
    }

    // both client and server has acces to their local_mrs after this
    return_mrs(&rdmas[0], &mut local_mrs, &mut remote_mrs, req_num, iamserver).await.unwrap();
    println!("-------- After Ops MRs: --------");
    println!("self-accessible local_mrs num: {}", local_mrs.len());
    println!("self-accessible remote_mrs num: {}", remote_mrs.len());
    print_mrs_wrapped(&rdmas[0], &local_mrs).await.unwrap();

    if iamserver {
        println!("Server done!");
    } else {
        println!("Client done!");
    }
}
