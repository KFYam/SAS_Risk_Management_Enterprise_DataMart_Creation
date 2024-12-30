# SAS_Risk_Management_Enterprise_DataMart_Creation
This repository demonstrates how to use SAS to build an enterprise-grade risk management datamart. It covers the end-to-end process of data extraction, transformation, and loading (ETL), along with key techniques for organizing and optimizing data for risk analysis.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Introduction

In the financial industry, effective risk management is crucial. This project provides a comprehensive guide on creating a risk management data mart using SAS, facilitating efficient data analysis and decision-making processes.

## Features

- End-to-end ETL process using SAS.
- Data quality reporting and self-check mechanisms.
- Performance optimization techniques for large datasets.
- Modular and reusable code structure.

## Project Structure

The repository is organized as follows:

```
SAS_Risk_Management_Enterprise_DataMart_Creation/
├── 00_COMMON/
├── 01_FACT_SCODE/
├── 02_SELFCHECK_SCODE/
├── 03_MART_SCODE/
├── 04_DQREPORT_SCODE/
├── 05_PERFORM_SCODE/
├── 00_MAIN_SCRIPT.sas
├── 01_FACT_MCODE.sas
├── 02_SELFCHECK_MCODE.sas
├── 03_MART_MCODE.sas
├── 04_DQREPORT_MCODE.sas
├── 05_PERFORM_MCODE.sas
├── 99_VERSION_HISTORY.txt
├── M02_YBX_SUBSET.sas
└── README.md
```

- **00_COMMON/**: Contains common utility macros and functions used across the project.
- **01_FACT_SCODE/**: Scripts related to fact table creation.
- **02_SELFCHECK_SCODE/**: Self-check and validation scripts.
- **03_MART_SCODE/**: Data mart creation scripts.
- **04_DQREPORT_SCODE/**: Data quality reporting scripts.
- **05_PERFORM_SCODE/**: Performance optimization scripts.
- **00_MAIN_SCRIPT.sas**: Main script orchestrating the ETL process.
- **99_VERSION_HISTORY.txt**: Changelog documenting project updates.

## Getting Started

### Prerequisites

- SAS 9.4 or later installed on your system.
- Basic understanding of SAS programming and ETL processes.

### Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/KFYam/SAS_Risk_Management_Enterprise_DataMart_Creation.git
   ```

2. **Navigate to the project directory**:

   ```bash
   cd SAS_Risk_Management_Enterprise_DataMart_Creation
   ```

3. **Set up your SAS environment**:

   - Ensure that the SAS system options and library references in the scripts match your local environment.
   - Modify the `libname` statements in the `00_MAIN_SCRIPT.sas` file to point to your data sources and target directories.

## Usage

1. **Execute the main script**:

   - Open `00_MAIN_SCRIPT.sas` in SAS.
   - Run the script to initiate the ETL process.

2. **Monitor the process**:

   - Check the SAS log for any errors or warnings.
   - Review the output data sets and reports generated in the specified directories.

3. **Customize the process**:

   - Modify the scripts in the `01_FACT_SCODE/`, `02_SELFCHECK_SCODE/`, etc., directories to adapt to your specific data sources and requirements.
