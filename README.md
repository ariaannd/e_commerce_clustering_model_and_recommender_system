# **E-Commerce Segmentation Analysis and Recommender System**

---

**Team Members**:
1. Aria Ananda (Data Engineer)
2. Muhammad Athaariq Ardi (Data Analyst)
3. Muhammad Irsyad Rafif (Data Scientist - Recommender System & Deployment)
4. Rezkyawan Saputra (Data Scientist - Clustering)


## Project Overview
This project focuses on performing segmentation analysis on e-commerce sales data. By employing K-Means Clustering, the objective is to segment customers effectively, providing insights for a recommender system application that tailors product recommendations to each cluster. Moreover, the project aims to assist the marketing team in crafting targeted campaigns for each segment, ultimately enhancing sales and revenue.

## Background
### Problem Statement
In the competitive e-commerce landscape, extracting actionable insights from historical sales data can inform product development and marketing strategies. However, the vast amount of unstructured sales data poses challenges in efficiently categorizing and analyzing customer purchase behavior for segmentation. 

An automated system is required to process this data effectively, providing structured analysis of customer segmentation and recommending products tailored to each segment. This project seeks to address the lack of structured analysis of customer segmentation based on e-commerce sales data and implement a recommender system to enhance sales performance.

### Objectives
- Develop an Automated Segmentation Model:
   - Build and train a K-Means Clustering model to accurately classify product sales into customer segments.
- Develop an Automated Recommender System:
   - Utilize the segmentation analysis to understand customer purchase tendencies and recommend products automatically for each segment.

### Dataset
The dataset used in this project is sourced from Kaggle.com, uploaded by Carrie. It can be accessed [here](https://www.kaggle.com/datasets/carrie1/ecommerce-data/data).

## Project Structure
### Workflow
The workflow is divided into three main phases, each assigned to different roles:

#### Data Engineering
- **Raw Data Loading**: Set up Apache Airflow DAG to load and store raw data in Postgres.
- **Fetch Data & Data Preprocessing**: Retrieve raw data from Postgres and preprocess it into cleaned data.
- **Clean Data Loading**: Store the cleaned data back into Postgres.

#### Data Analysis
- **Data Interpretation**: Analyze the cleaned data to extract insights.
- **Visualization**: Create visual representations of the analysis for easier interpretation and presentation on Tableau.
- **Reporting**: Compile findings and insights into a comprehensive report.

#### Data Science
- **Model Development**: Utilize the cleaned data to create a K-Means Clustering model for customer segmentation.
- **Model Optimization**: Fine-tune and optimize the clustering model.
- **Recommender System**: Develop a recommender system based on the clustering model to offer products tailored to each customer segment.
- **Model Deployment**: Build an application that integrates the developed models to automate customer segmentation and product recommendations.

## Tools
- **Docker**: Containerization to ensure reproducibility of the data pipeline.
- **Apache Airflow**: Orchestration and automation of the data pipeline.
- **PostgreSQL**: Data storage and retrieval.
- **K-Means Clustering**: Used for building and training the customer segmentation model.
- **Python**: Primary programming language for data processing, analysis, and modeling.
- **Streamlit & Huggingface**: Building an application for the model and recommender system.

## Setup and Installation
To replicate this project, ensure you have the following prerequisites:
1. Dataset files, downloadable [here](#dataset).
2. Docker.
3. Python.
4. Environment Configuration: `.env` file for the Airflow containers to access PostgreSQL.

To replicate the project:
1. Clone this repository:
```bash
git clone git@github.com:FTDS-assignment-bay/p2-final-project-p2-final-project-ftds-013-hck-group-003.git
```
2. Compose the containers:
```bash
docker compose -f airflow.yaml up
```
3. Access the Airflow webserver and set up the database credentials for PostgreSQL.
4. Trigger the DAG.
5. Download the cleaned data from the PostgreSQL database.
6. Run the `modeling.ipynb` file to create the models.

Alternatively, the models are available in the repository, and a deployed model can be found on [our Huggingface](https://huggingface.co/spaces/Rafion101/Final_Project_Customer_Segmentation_and_Recomendation_System).

Visualizations are accessible on [our Tableau](https://public.tableau.com/app/profile/muhammad.athaariq2169/viz/FinalprojectFTDS/Final2?publish=yes).

## Conclusions & Further Improvement
The project's outcome provides valuable insights and solutions, although there is room for further development:

**Conclusion & Business Recommendations**  
- Establish a membership program with a reward-based system to incentivize frequent shopping, thereby boosting sales and revenue.
- Implement popular promotions targeting low-spending customer segments.
- Execute targeted marketing campaigns aimed at high-spending customers to improve retention rates and decrease cancellation rates.
- Initiate marketing campaigns, starting in November.

**Further Improvements**
- Conduct analysis and segmentation for locations beyond the UK, focusing on countries like Germany.
- Enhance data processing for more granular analysis of purchased items.
- Refine recommendations to consider both clusters and specific items purchased by each cluster.
- Improve the user interface and recommendation system based on clusters and product types for model deployment.