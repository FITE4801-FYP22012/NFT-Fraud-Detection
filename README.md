# FITE4801 - Non-Fungible Token Fraud Detection System

This repository contains the code for the deployment of our FITE4801 project.

## Project Description

This project aims to use machine learning to better classify NFTs and detect which tokens may be suspicious based on their collection metadata and activities taking place on the blockchain.

In partnership with Dotted Company Limited (DTTD), a local start-up that is developing a mobile-first NFT social platform, we are creating a system that will be integrated into their platform to provide real-time classifications for NFTs in their mobile app.

## Directory Structure
├── cfg
│   ├── model_build.yaml        # Configuration file for model building
│   └── model_deploy.yaml       # Configuration file for model deployment
├── docker
│   ├── mlflow_endpoint         # Script to deploy MLflow endpoint
│   │   └── mlflow_pyfunc.sh
│   ├── processing              # Dockerfile for data processing sagemaker job
│   │   └── Dockerfile
│   └── training                # Dockerfile for training sagemaker job
│       └── Dockerfile
├── experiment                  # Notebooks for data exploration and model building
├── scripts                     # Scripts for deploying stack
├── src                         
│   ├── model_build             # Source code for model building
│   │   ├── data_preparation    # pipeline for data preparation
│   │   └── training            # pipeline for model training
│   └── model_deploy            # Source code for model deployment
├── test

## Getting Started

To get started, clone this repository and install the required packages in a virtual environment:
```
pip install -r requirements.txt
```

Then with aws cli installed, run the following command to configure your aws credentials:
```
aws configure
```


## Deployment

### Prerquisites
- AWS account and CLI
- A a central MLflow tracking server
- Connecting GitHub Actions to your AWS account
- Setting up GitHub repo secrets for the project

### SageMaker jobs
For processing job:
```
sh scripts/build_and_push.sh <ecr-processing-repo-name> ./docker/processing
```
For training job:
```
sh scripts/build_and_push.sh <ecr-training-repo-name> ./docker/training
```
### Inference endpoint
To deploy the inference CDK stack, run the following command:
```bash
. scripts/deploy_stack.sh
```
