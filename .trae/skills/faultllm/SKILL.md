---
name: "faultllm"
description: "FaultLLM project assistant for Hadoop cluster fault detection using LLM. Invoke when user asks about fault detection, NetLLM-style architecture, multi-modal encoding, or training/inference on GPU server A40-123."
---

# FaultLLM Project Skill

This skill helps with the FaultLLM project - a Hadoop cluster fault detection system based on Large Language Models (LLM), following the NetLLM framework design.

## Project Overview

FaultLLM detects and classifies 9 types of faults in Hadoop clusters:
- **normal** (0): Normal operation
- **data_skew** (1): Data skew fault
- **data_bloat** (2): Data bloat fault
- **task_fail** (3): Task failure fault
- **long_tail** (4): Long-tail task fault
- **wait_time** (5): Wait time anomaly
- **runtime_delta** (6): Runtime anomaly
- **exit_time** (7): Exit time anomaly
- **network_latency** (8): Network latency fault

## Architecture

Following NetLLM design patterns:

### 1. Multi-Modal State Encoder (`state_encoder.py`)
- **MetricEncoder**: Processes time-series metrics (CPU, memory, disk, network)
- **LogEncoder**: Processes log semantic features
- **TopologyEncoder**: Encodes cluster topology information
- **CrossModalAttention**: Interactive attention layer for semantic-metric alignment
- **TemporalAlignment**: Timestamp alignment between metrics and logs

### 2. Model (`models.py`)
- **FaultLLMModelSimple**: Lightweight model without LLM backbone
- **FaultLLMModel**: Full model with LLM backbone (supports GPT2, LLaMA, Qwen, Mistral)
- **LoRA Support**: Low-rank adaptation for efficient fine-tuning

### 3. Data Processing (`dataset.py`)
- **TimestampAligner**: Aligns metrics and logs by timestamp
- **FaultDataset**: PyTorch Dataset with padding and truncation
- Time-series alignment with tolerance window

### 4. Training (`trainer.py`)
- Mixed precision training (AMP)
- Gradient accumulation
- Learning rate scheduling
- Checkpoint saving

## GPU Server Environment

### Connection
```bash
ssh A40-123  # Connect to worker-01
```

### Environment Setup
```bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cxw
cd /data/dds-data/cxw/fault_detection_v2
```

### Available Models
- Qwen model is installed in `/data/dds-data/cxw/` directory
- 6x NVIDIA A40 GPUs (46GB each)

## Training Commands

### Simple Model (Quick Test)
```bash
python3 run.py --mode train --model_type simple --data_source simulated --num_samples 500 --epochs 20
```

### With Real Data
```bash
python3 run.py --mode train --model_type simple --data_source json --data_json training_dataset.json --epochs 50
```

### With LLM (7B/8B models)
```bash
python3 run.py --mode train --model_type qwen2-7b --use_lora --data_source json --data_json training_dataset.json
```

## Data Sources

### Local Processing (on cpf-1)
1. **Fault Injection**: `/project/data/data_scripts/`
2. **Data Collection**: `/tmp/fault_verification/`
3. **Data Processing**: `/project/code/fault_detection/`

### GPU Server (A40-123)
1. **Code**: `/data/dds-data/cxw/fault_detection_v2/`
2. **Training**: Run on GPU server only
3. **Data**: Sync from local using rsync

## Key Design Patterns (Following NetLLM)

1. **Multi-branch Encoding**: Separate encoders for different feature types
2. **Cross-Modal Attention**: Not simple concatenation, but interactive attention
3. **Time Embedding**: Learnable positional encoding for timesteps
4. **LoRA Fine-tuning**: Efficient adaptation of large models
5. **Early Stopping**: Support for stopping at specific layers

## File Structure

```
/project/code/fault_detection/
├── config.py              # Configuration (fault types, dimensions)
├── state_encoder.py       # Multi-modal state encoder
├── models.py              # Model definitions
├── dataset.py             # Data processing with timestamp alignment
├── trainer.py             # Training loop
├── run.py                 # Main entry point
├── process_real_data.py   # Process collected data
├── generate_training_data.py  # Data augmentation
└── validate_pipeline.py   # Quick validation without PyTorch
```

## SSH Configuration

```ssh
Host jumpbox-Gcluster
    HostName 222.200.180.22
    User user
    IdentityFile /home/ubuntu/.ssh/id_rsa

Host A40-123
    HostName 33.33.33.123
    User dds
    IdentityFile /home/ubuntu/.ssh/id_rsa
    ProxyCommand ssh -W %h:%p jumpbox-Gcluster
```

## Important Notes

1. **Data operations on local machine** (cpf-1)
2. **Training on GPU server** (A40-123)
3. **Use `cxw` conda environment**
4. **Sync code before training**: `rsync -avz -e ssh /project/code/fault_detection/ A40-123:/data/dds-data/cxw/fault_detection_v2/`
