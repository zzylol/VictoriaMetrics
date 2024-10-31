# promsketch

This repository provides PromSketch package for Prometheus and VictoriaMetrics.


### Install Dependencies
```
# installs Golang
wget https://go.dev/dl/go1.22.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

```
# installs nvm (Node Version Manager)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash
# download and install Node.js (you may need to restart the terminal)
nvm install 20
```

### Datasets
* Goolge Cluster Data v1: https://github.com/google/cluster-data/blob/master/TraceVersion1.md
* Power dataset: https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set?resource=download
* CAIDA traces: https://www.caida.org/catalog/datasets/passive_dataset_download/

### Run EHUniv test
```
cd promsketch
go test -v -timeout 0 -run ^TestExpoHistogramUnivMonOptimizedCAIDA$ github.com/froot/promsketch
```

### Run EHKLL test
```
cd promsketch
go test -v -timeout 0 -run ^TestCostAnalysisQuantile$ github.com/froot/promsketch
```

### Integration with Prometheus

```
git clone git@github.com:zzylol/prometheus-sketches.git
cd prometheus-sketches
go get github.com/froot-netsys/promsketch
go mod tidy
```
Compile:
```
make build
```

### Integration with VictoriaMetrics

```
git clone git@github.com:zzylol/VictoriaMetrics.git
cd VictoriaMetrics
go get github.com/froot-netsys/promsketch
go mod tidy
go mod vendor
```
Compile:
```
cd VictoriaMetrics
make victoria-metrics
make vmalert
```
