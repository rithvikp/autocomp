# Setup
In order to run any of these benchmarks, you must first set up your GCP account.
The experiments in total should cost somewhere in the low hundreds of dollars, but the cost of each experiment should be trivial. Each experiment for each protocol configuration has ~6 client configurations, each 1.5-2 minutes, repeated 3 times, using at most ~100 VMs, totaling around 1 hour per run, times 12+ configurations.

This setup is written assuming everything will be launched in the `<zone>` zone in the `<project>` project under the username `<username>`. Substitute those values in your own setup.


## Preliminary setup
We assume you already have a GCP account and have an associated project with billing enabled.

### Enable Compute Engine API
This is necessary for launching VMs. If you don't know how to enable the API, follow instructions [here](https://support.google.com/googleapi/answer/6158841?hl=en).

### Set up a VPC network
In order for the VMs to communicate with each other, you need a VPC network.
Go [here](https://console.cloud.google.com/networking/networks/list) and create one:
- Name: `eval-network`
- Subnet creation mode: Automatic
- Firewall rules: eval-network-allow-ssh

Use the default option for all other fields.

### Install gcloud
On your personal computer, [install gcloud](https://cloud.google.com/sdk/docs/install).
Run `gcloud auth login` and log into your GCP account.


## Setting up the NFS VM
We will create a VM to act as the Network File System (NFS). This will act as the shared log between all benchmark nodes.

### Create the VM
Go [here](https://console.cloud.google.com/compute/instances) and click "Create Instance":
- Name: `eval-nfs`
- Machine configuration: General purpose, N2
- Machine type: n2-standard-2
- Boot disk -> Change -> Operating system: Ubuntu

Other than the name, the options don't really matter, as long as the machine has enough disk and good enough specs so it won't slow down the experiment. The default 10GB of disk is enough.

### Set up SSH
On your personal computer, run `gcloud compute ssh --zone "<zone>" "eval-nfs" --project "<project>"`.  
Google will ask you to make a SSH password; make one that you can remember.

### Install NFS server
On the VM, run the following commands:
```bash
sudo apt install -y nfs-kernel-server
sudo mkdir /share
sudo chown nobody:nogroup /share
sudo chmod 777 /share
echo "/share *(rw,sync,no_subtree_check)" | sudo tee -a /etc/exports
sudo systemctl restart nfs-kernel-server
```

### Set up the firewall
Go [here](https://console.cloud.google.com/net-security/firewall-manager/firewall-policies) and click "Create Firewall Rule" so all other VMs can access the NFS, **replacing the IPv4 ranges with your own**:
- Name: `firewall-nfs`
- Network: `eval-network`
- Targets: All instances in the network
- Source filter: IPv4 ranges
- Source IPv4 ranges: 10.128.0.0/16
- Protocols and ports: Allow all

Here's how to find your own IPv4 ranges:
1. Go to your [VM instances](https://console.cloud.google.com/compute/instances).
2. Find the Internal IP of `eval-nfs`. Mine is 10.128.0.3.
3. Copy the first two octets (10.128 for me), then add 0.0/16 to the end. That's how I arrived at 10.128.0.0/16.


## Setting up the primary VM
This is the machine from which all experiments will be launched. It is also the development machine; if you edit the code in `rust/examples` from this machine with the VSCode rust-analyzer extension installed, you should get autocomplete.

### Create the VM
Go [here](https://console.cloud.google.com/compute/instances) and click "Create Instance":
- Name: `eval-primary`
- Machine configuration: General purpose, N2
- Machine type: n2-standard-16
- Boot disk -> Change -> Operating system: Ubuntu
- Boot disk -> Change -> Size (GB): 50
- Access scopes: Allow full access to all Cloud APIs

Installing dependencies on this machine will require a lot of space, hence the 50 GBs. It needs full access to all cloud APIs in order to launch additional VMs.


### Set up SSH
On your personal computer, run `gcloud compute ssh --zone "<zone>" "eval-primary" --project "<project>"`.

### Install Scala, Python, and Prometheus, and setting up SSH keys
We first install Java and Scala in order to run the Scala code.
```bash
sudo apt update
sudo apt -y upgrade
curl https://raw.githubusercontent.com/mwhittaker/vms/master/install_java8.sh -sSf | bash
wget https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux-static.gz
gzip -d cs-x86_64-pc-linux-static.gz
chmod +x cs-x86_64-pc-linux-static
./cs-x86_64-pc-linux-static setup
```
Answer yes to all questions in the last command.

At the top of `~/.bashrc`, add the line `source ~/.bash_path`.

Now install `conda` for Python.
```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-py37_23.1.0-1-Linux-x86_64.sh
bash Miniconda3-py37_23.1.0-1-Linux-x86_64.sh
```
Jump to the end of the terms with `q`, accept it, then install at the default location, and answer yes at the end as well. Create the `conda` environment and activate it with:
```bash
source ~/.bashrc
conda create -n autocomp python=3.7
echo "conda activate autocomp" >> ~/.bashrc
source ~/.bashrc
```
Answer yes. You should now see `(autocomp)` at the beginning of your command line.

In order to launch a ton of programs successfully, increase the `MaxSessions` and `MaxStartups` in `/etc/ssh/sshd_config` to `100` and `100:30:200`, respectively, with `sudo vim`. Then run:
```bash
source ~/.profile
source ~/.bashrc
```

Now generate an SSH key so the machines can SSH into each other in order to launch experiments, replacing `<email>` with whichever email you prefer:
```bash
ssh-keygen -t ed25519 -C "<email>"
```
Don't enter any additional info (click enter through every question).  
Copy the public key into `~/.ssh/authorized_keys` with:
```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
```
Then copy the same public key in `~/.ssh/id_ed25519.pub` to GCP:
1. Go to [VM instances](https://console.cloud.google.com/compute/instances).
2. Click `eval-primary`.
3. Click "Edit".
4. Scroll down to "SSH Keys", then click "Add item", and paste in the full key.
5. Click "Save".

We will now install Prometheus for profiling. Find the latest version [here](https://prometheus.io/download/) and copy the link for the Linux distribution. As of writing the latest version is [2.49.1](https://github.com/prometheus/prometheus/releases/download/v2.49.1/prometheus-2.49.1.linux-amd64.tar.gz).
```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.49.1/prometheus-2.49.1.linux-amd64.tar.gz
tar xvfz prometheus-*.tar.gz
```
Add Prometheus to the path by appending `export PATH="/home/<username>/prometheus-2.49.1.linux-amd64:$PATH"` to the top of `~/.bashrc`.


### Connecting to NFS server

We can connect this machine to the NFS server we set up earlier. Replace `<eval-nfs-ip>` with the **internal** IP address of the NFS server set up earlier. You can find this address [here](https://console.cloud.google.com/compute/instances).

Note: Continue executing these commands on `eval-primary`.
```bash
sudo apt install -y nfs-common
sudo mkdir /mnt/nfs
sudo mount -t nfs -o vers=4 -o resvport <eval-nfs-ip>:/share /mnt/nfs
mkdir /mnt/nfs/tmp
echo "<eval-nfs-ip>:/share               /mnt/nfs      nfs auto,nofail,noatime,nolock,intr,tcp,actimeo=1800 0 0" | sudo tee -a /etc/fstab
```


### Create the worker image
At this point we will need to stop `eval-primary` in order to create the VM image that all the workers will be based on. Exit the terminal, stop `eval-primary` on the GCP console and wait for it to stop.

Go [here](https://console.cloud.google.com/compute/images) and click "Create Image":
- Name: `worker-image`
- Source disk: `eval-primary`
- Location: Regional

Once the image is created, you can start `eval-primary` and SSH into it again.

### Installing Rust, Hydroflow, and Python dependencies
We will first install Rust:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```
Select option 1 (install with default settings).

Now clone [Hydroflow](https://github.com/hydro-project/hydroflow) and find the branch this repo depends on:
```bash
git clone https://github.com/hydro-project/hydroflow.git
cd hydroflow
git fetch origin rithvik/autocomp_adjustments
git checkout rithvik/autocomp_adjustments
```

Install the necessary dependencies, including terraform:
```bash
sudo apt install -y build-essential libssl-dev pkg-config
wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update
sudo apt install -y terraform
```

We will also install the exact Rust nightly version that Hydroflow depends on:
```bash
rustup toolchain install nightly-2023-04-18
rustup target add --toolchain nightly-2023-04-18 x86_64-unknown-linux-musl
```

Compile `hydro_cli` and load it into the local Python environment:
```bash
pip install maturin==0.14
cd hydro_cli
maturin develop
```

It's finally time to compile this Repo. Clone this repo, then install the protobuf compiler and necessary Python dependencies:
```bash
cd ~
git clone https://github.com/rithvikp/autocomp.git
cd autocomp
sudo apt install -y protobuf-compiler
pip install -r benchmarks/requirements.txt
```

Compile the Scala code, and move it to the shared directory. This will take a while on the first run:
```bash
./scripts/assembly_without_tests.sh
cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar /mnt/nfs/tmp
```
> [!IMPORTANT]  
> Scala must be recompiled, then saved to `/mnt/nfs/tmp` EACH TIME the Scala code or protobuf files are updated. Otherwise the changes will not be reflected.

Your VM should be ready for experimenting! When you're done using the VMs, stop `eval-nfs` and `eval-primary` to save some money. On startup, start `eval-nfs` first before starting `eval-primary`.

Head to [Protocols](PROTOCOLS.md) to see how to run the benchmarks.