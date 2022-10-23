# pipelines

This is a repository for developing Airflow pipeline scripts

## Getting Started

### Prerequisites

- Python 3 installed

### Installation

You can skip this step if you already installed Airflow

- Execute `source install_airflow.sh` to install Airflow

### Configuration for local dev

- Execute `source airflow_cfg.sh` to override Airflow configuration

### Running the airflow

- Execute `airflow standalone` to start Airflow

### Loading global configuration

1. Open Airflow UI at http://localhost:8080
2. Click on Admin -> Variables
3. Click on Choose File and select `global_config.json`