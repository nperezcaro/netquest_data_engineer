---
Author: Nicolás Pérez Caro
Status: WIP
---
This repository contains the Coding Assesment for the Data Engineer role in Netquest.

# Explanation
The code workflow is a follows:

    1. Create two DataFrames using the inputs and mappings csv files.

    2. Using the mappings DF, create 3 dictionaries for:
        - Channel
        - Language
        - CustomFields

    3. Map changes to Channel & Language fields given the Channel & Language dicts build on point 2.

    4. Map changes to CustomFields fields given the CustomFields dict build on point 2.

    5. After the result of point 4, dimensions might be duplicated. So aggregation is done using the dimension columns to sum the facts.

    6. Finally the TotalPointsGained are calculated by summing the PointsGained over the id column.

graph TD
    A[Start: Read inputs.csv and mappings.csv into DataFrames] --> B[Create dictionaries from mappings DataFrame]
    B --> B1[Dictionary 1: Channel]
    B --> B2[Dictionary 2: Language]
    B --> B3[Dictionary 3: CustomFields]

    B1 --> C[Map Channel & Language fields using Channel & Language dictionaries]
    B2 --> C
    B3 --> D[Map CustomFields fields using CustomFields dictionary]

    C --> E[Check for duplicate dimensions]
    D --> E

    E --> F[Aggregate data: Sum facts based on dimension columns]
    F --> G[Calculate TotalPointsGained by summing PointsGained over id column]
    G --> H[Output final DataFrame]

## Run the code
### Container
The repository contains a Dockerfile, so if you have access to a Docker engine, you can run the code by executing:

```bash
docker build -t {desired_tag} .
```

```bash
docker run --name {desired_container_name} {desired_tag}
```

Since the run is not being done detached you'll be able to see the logs.
### Local
Clone the repo:
```bash
git clone https://github.com/nperezcaro/netquest_data_engineer.git
```

Navigate to the project directory:
```bash
cd netquest_data_engineer/
```

Create a virtual environment:
```bash
python -m venv venv
```

Activate the virtual environment:
```bash
venv\Scripts\activate
```

Update pip and install dependencies:
```bash
python -m pip install --upgrade pip && pip install -r requirements.txt
```

To run the code:
```bash
python main.py
```

## CI Pipeline
The repository contains a CI Pipeline that will execute the test and fail under a 85% coverage.
