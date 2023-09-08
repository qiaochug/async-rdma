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
    ops::DerefMut};
use rand::Rng;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

async fn prepare_mrs(rdma: &Rdma, local_mrs: &mut Vec<LocalMr>, remote_mrs: &mut Vec<RemoteMr>,
    req_num: usize, req_size: usize, iamserver: bool) -> io::Result<()>{

    for i in 0..req_num {
        // Allocating local memory
        let mut lmr = rdma.alloc_local_mr(Layout::array::<u8>(req_size).unwrap())?;

        // write data into lmr
        let i_u8 : u8 = u8::try_from(i % 10).ok().unwrap();
        let fill: u8 = if iamserver { i_u8 } else { 10 - i_u8 };
        for byte in lmr.get_mut(0..req_size).unwrap().as_mut_slice().iter_mut() {
            *byte = fill;
        }

        if iamserver {
            // server send local mrs to client
            rdma.send_local_mr(lmr).await?;
        } else {
            // client saves local mrs
            local_mrs.push(lmr);

            // client receives and save remote mrs
            let rmr = rdma.receive_remote_mr().await?;
            remote_mrs.push(rmr);
        }
    }

    Ok(())
}

async fn return_mrs(rdma: &Rdma, local_mrs: &mut Vec<LocalMr>, remote_mrs: &mut Vec<RemoteMr>,
    req_num: usize, iamserver: bool) -> io::Result<()>{

    for i in 0..req_num {
        if iamserver {
            // server receives local mrs
            let lmr = rdma.receive_local_mr().await?;
            local_mrs.push(lmr);
        } else {
            let rmr = remote_mrs.remove(0);
            // client sends remote mrs
            rdma.send_remote_mr(rmr).await?;
        }
    }

    Ok(())
}

async fn resend_mrs(rdma: &Rdma, local_mrs: &mut Vec<LocalMr>, remote_mrs: &mut Vec<RemoteMr>,
    req_num: usize, iamserver: bool) -> io::Result<()>{

        for i in 0..req_num {
    
            if iamserver {
                let lmr = local_mrs.remove(0);
                // server re-send local mrs to client
                rdma.send_local_mr(lmr).await?;
            } else {
                // client receives and save remote mrs
                let rmr = rdma.receive_remote_mr().await?;
                remote_mrs.push(rmr);
            }
        }

    Ok(())
}

async fn print_mrs(rdma: &Rdma, local_mrs: &Vec<LocalMr>, req_num: usize) -> io::Result<()>{

    for i in 0..req_num {
        let slice = local_mrs[i].as_slice();
        println!("Index {} First {:?} Last {:?} Size {}", i, slice.get(0), slice.get(slice.len() - 1), slice.len());
    }

    Ok(())

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
    println!("local_mrs num: {}", local_mrs.len());
    println!("remote_mrs num: {}", remote_mrs.len());

    // // both client and server has acces to their local_mrs after this
    return_mrs(&rdmas[0], &mut local_mrs, &mut remote_mrs, req_num, iamserver).await.unwrap();
    println!("local_mrs num: {}", local_mrs.len());
    println!("remote_mrs num: {}", remote_mrs.len());

    print_mrs(&rdmas[0], &local_mrs, req_num).await.unwrap();

    resend_mrs(&rdmas[0], &mut local_mrs, &mut remote_mrs, req_num, iamserver).await.unwrap();
    println!("local_mrs num: {}", local_mrs.len());
    println!("remote_mrs num: {}", remote_mrs.len());

    if iamserver {
        println!("Server up!");
    } else {
        println!("Client up!");
    }

    // let mut local_mrs = Vec::with_capacity(req_num);
    // let mut remote_mrs = Vec::with_capacity(req_num);
    // // only the client has access to local_mrs and remote_mrs for now
    // prepare_mrs(&rdma, &mut local_mrs, &mut remote_mrs, req_num, req_size, iamserver).await.unwrap();
    // println!("local_mrs num: {}", local_mrs.len());
    // println!("remote_mrs num: {}", remote_mrs.len());

    // let mut lmr1_i = local_mrs.remove(0);
    // let lmr1_o = Arc::new(lmr1_i);
    // let rmr1_o = Arc::new(remote_mrs.remove(0));
    // let lmr1 = Arc::clone(&lmr1_o);
    // let rmr1 = Arc::clone(&rmr1_o);

    // let rdma_arc = Arc::new(rdma);
    // let rdma1 = Arc::clone(&rdma_arc);
    // let rdma2 = Arc::clone(&rdma_arc);

    // let jh1 = tokio::spawn(async move {rdma1.read(&mut lmr1, &rmr1).await;});
    
    // let mut join_handles = Vec::with_capacity(req_num);
    // let start_time;
    // if !iamserver {
    //     let mut lmr1_i = local_mrs.remove(0);
    //     let mut lmr1 = Arc::new(RwLock::new(lmr1_i));
    //     let mut lmr2_i = local_mrs.remove(0);
    //     let mut lmr2 = Arc::new(RwLock::new(lmr2_i));
    //     let rmr1_o = Arc::new(remote_mrs.remove(0));
    //     let rmr2_o = Arc::new(remote_mrs.remove(0));

    //     let mut lmr_ls = Vec::with_capacity(2);
    //     lmr_ls.push(lmr1);
    //     lmr_ls.push(lmr2);

    //     // let mut lmr1_e = Arc::clone(&lmr1);
    //     // let mut lmr2_e = Arc::clone(&lmr2);
    //     let rmr1 = Arc::clone(&rmr1_o);
    //     let rmr2 = Arc::clone(&rmr2_o);

    //     {
    //         let lmr1_er = lmr_ls[0].read();
    //         let slice = lmr1_er.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //         let lmr2_er = lmr_ls[1].read();
    //         let slice = lmr2_er.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //     }

    //     let rdma_arc = Arc::new(rdma);
    //     let rdma1 = Arc::clone(&rdma_arc);
    //     let rdma2 = Arc::clone(&rdma_arc);
        
    //     let mut lmr1_n = Arc::clone(&lmr_ls[0]);
    //     let mut lmr2_n = Arc::clone(&lmr_ls[1]);
    //     let jh1 = tokio::spawn(async move {
    //         // let lmr_w = lmr1.write().unwrap();
    //         // let lmr = binding.deref_mut();
    //         rdma1.read(lmr1_n.write().deref_mut(), rmr1.as_ref()).await;
    //         let lmr_r = lmr1_n.read();
    //         let slice = lmr_r.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //         // drop(slice);
    //     });
    //     let jh2 = tokio::spawn(async move {
    //         // let lmr_w = lmr2.write().unwrap();
    //         // let lmr = binding.deref_mut();
    //         rdma2.read(lmr2_n.write().deref_mut(), rmr2.as_ref()).await;
    //         let lmr_r = lmr2_n.read();
    //         let slice = lmr_r.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //         // drop(slice);
    //     });
    //     let start_time = Instant::now();
    //     // rdma.read(&mut lmr1, &rmr1).await;
    //     // rdma.read(&mut lmr2, &rmr2).await;
    //     jh1.await;
    //     jh2.await;
    //     {
    //         let lmr1_er = lmr_ls[0].read();
    //         let slice = lmr1_er.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //         let lmr2_er = lmr_ls[1].read();
    //         let slice = lmr2_er.as_slice();
    //         println!("First {:?} Last {:?} Size {}", slice.get(0), slice.get(slice.len() - 1), slice.len());
    //     }
    //     let end_time = Instant::now();
    //     let elapsed_time = end_time - start_time;

    //     let elapsed_seconds = elapsed_time.as_secs();
    //     println!("Elapsed time: {} seconds", elapsed_seconds);
    //     println!("Average Elapsed time: {} seconds", elapsed_seconds as f64/2.0);

    //     let elapsed_nanoseconds = elapsed_time.as_nanos();
    //     println!("Elapsed time: {} nanoseconds", elapsed_nanoseconds);
    //     println!("Average Elapsed time: {} nanoseconds", elapsed_nanoseconds as f64/2.0);
    // } else {
    //     tokio::time::sleep(Duration::new(5, 0)).await;
    // }
    //TODO: add sync
    

    // // both client and server has acces to their local_mrs after this
    // return_mrs(&rdma, &mut local_mrs, &mut remote_mrs, req_num, iamserver).await.unwrap();
    // println!("local_mrs num: {}", local_mrs.len());
    // println!("remote_mrs num: {}", remote_mrs.len());

    // print_mrs(&rdma, &local_mrs, req_num).await.unwrap();

    

    // let mut rng = rand::thread_rng();
    // let mut operation_list: Vec<i32> = Vec::new();

    // // Generate random 0s and 1s to represent read (1) or write (0)
    // let mut read_num = 1;
    // for _ in 0..req_num {
    //     let random_value: f64 = rng.gen();
    //     if random_value < read_pct {
    //         operation_list.push(1);
    //         read_num += 1;
    //     } else {
    //         operation_list.push(0);
    //     }
    // }
    // let effective_read_pct = read_num as f64/req_num as f64;
    // println!("Effective Read_pct {:.2}:", effective_read_pct);

    // for i in 0..req_num {

    //     if operation_list[i] == 1 {
    //         println!("Performing read operation");
    //     } else {
    //         println!("Performing write operation");
    //     }

    // }
    if iamserver {
        println!("Server done!");
    } else {
        println!("Client done!");
    }
}
