# Scouting Report: Professional Football Analytics Platform

## Table of Contents

- [Project Overview](#project-overview)
  - [Key Objectives](#key-objectives)
- [Core Technologies](#core-technologies)
  - [Data Collection & Web Scraping](#data-collection--web-scraping)
  - [Data Processing & Analysis](#data-processing--analysis)
  - [Visualization](#visualization)
  - [Data Management](#data-management)
- [Data Preparation and Visualization](#data-preparation-and-visualization)
  - [Data Collection Pipeline](#data-collection-pipeline)
  - [Data Cleaning and Transformation](#data-cleaning-and-transformation)
  - [Visualization and Exploratory Data Analysis](#visualization-and-exploratory-data-analysis)
- [Report Structure and Key Findings](#report-structure-and-key-findings)
  - [Dataset Structure](#dataset-structure)
  - [Player Role Clustering](#player-role-clustering)
  - [Key Analytical Insights](#key-analytical-insights)
  - [Output Deliverables](#output-deliverables)
- [Project Structure](#project-structure)
- [Technical Highlights](#technical-highlights)
  - [Robust Error Handling](#robust-error-handling)
  - [Scalable Architecture](#scalable-architecture)
  - [Production-Ready Data Quality](#production-ready-data-quality)
- [Future Enhancements](#future-enhancements)
- [Dependencies](#dependencies)
- [Development History](#development-history)
  - [Phase 1: Initial Data Acquisition (August 2024)](#phase-1-initial-data-acquisition-august-2024)
  - [Phase 2: Core Data Cleaning Pipeline (May-June 2025)](#phase-2-core-data-cleaning-pipeline-may-june-2025)
  - [Phase 3: Advanced Analysis and Machine Learning (June 2025)](#phase-3-advanced-analysis-and-machine-learning-june-2025)
  - [Phase 4: System Refinement and Data Expansion (July-September 2025)](#phase-4-system-refinement-and-data-expansion-july-september-2025)
  - [Phase 5: Documentation and Finalization (November-December 2025)](#phase-5-documentation-and-finalization-november-december-2025)
- [Usage](#usage)

---

## Project Overview

The **Scouting Report** is a comprehensive football analytics platform designed to generate actionable insights and detailed reports for player performance analysis across Europe's Big 5 leagues (Premier League, La Liga, Serie A, Bundesliga, and Ligue 1). Unlike predictive models, this project focuses on **data-driven analysis and reporting** to identify player patterns, role classifications, and performance trends spanning seven seasons (2017-2024).

The platform processes multi-dimensional player statistics including standard metrics, defensive actions, passing patterns, shooting efficiency, and goalkeeping performance. Through advanced data preparation, exploratory analysis, and unsupervised learning techniques, the system generates comprehensive scouting reports that enable data-informed decision-making for player evaluation, role identification, and comparative analysis.

### Key Objectives

- **Comprehensive Data Collection**: Automated web scraping of player and team statistics from FBref.com across multiple seasons
- **Robust Data Processing**: Systematic cleaning, normalization, and integration of heterogeneous football statistics
- **Player Role Classification**: Unsupervised machine learning to identify distinct player archetypes and playing styles
- **Insight Generation**: Statistical analysis and visualization to uncover performance patterns and trends
- **Report Generation**: Structured outputs for technical analysis and scouting purposes

---

## Core Technologies

### Data Collection & Web Scraping
- **requests**: HTTP library for web scraping with retry mechanisms and session management
- **BeautifulSoup4**: HTML parsing and table extraction from FBref.com
- **Selenium**: Browser automation for dynamic content (shooting statistics)
- **fuzzywuzzy**: Fuzzy string matching for player name reconciliation

### Data Processing & Analysis
- **pandas**: Data manipulation, cleaning, merging, and transformation
- **numpy**: Numerical computations and array operations
- **scikit-learn**: 
  - `StandardScaler`: Feature normalization for clustering
  - `KMeans`: Unsupervised clustering for player role identification
  - `PCA`: Dimensionality reduction for visualization
  - `VarianceThreshold`: Feature selection to remove low-variance columns

### Visualization
- **matplotlib**: Statistical plotting and visualization
- **seaborn**: Advanced statistical visualizations including correlation heatmaps and pair plots

### Data Management
- **Python 3.x**: Core programming language
- **Jupyter Notebooks**: Interactive development and analysis environment

---

## Data Preparation and Visualization

### Data Collection Pipeline

The project implements a sophisticated multi-source data collection system:

#### 1. **Web Scraping Infrastructure**
- **Retry Logic**: HTTPAdapter with exponential backoff for handling rate limits and server errors
- **User-Agent Rotation**: Browser header management to minimize blocking
- **Rate Limiting**: Random delays (5-10 seconds) between requests to respect server resources
- **Multi-Format Support**: Handles both static HTML (requests) and dynamic JavaScript content (Selenium)

#### 2. **Data Sources Collected**
- **Standard Statistics**: Goals, assists, expected goals (xG), expected assists (xAG), progressive actions
- **Defensive Metrics**: Tackles, interceptions, blocks, clearances, defensive actions by field third
- **Passing Statistics**: Completion rates by distance (short/medium/long), progressive passes, key passes, passes into final third
- **Shooting Data**: Shot volume, shot on target percentage, goals per shot, expected goals per shot
- **Goalkeeping Stats**: Saves, clean sheets, goals against, save percentage
- **Squad-Level Data**: Team statistics and wage information across multiple currencies (EUR, GBP, USD)

#### 3. **Temporal Coverage**
- **Seasons**: 2017-2018 through 2023-2024 (7 complete seasons)
- **Leagues**: All Big 5 European leagues
- **Player Records**: ~14,000+ player-season observations after filtering

### Data Cleaning and Transformation

#### 1. **Header Standardization**
- Identifies correct header rows using identifier patterns (e.g., "Rk")
- Removes duplicate header rows embedded in data
- Handles inconsistent column naming across seasons

#### 2. **Text Cleaning**
- **Nation Codes**: Extracts standardized 3-letter country codes (e.g., "es ESP" → "ESP")
- **League Names**: Normalizes competition names (e.g., "eng Premier League" → "Premier League")
- **Squad Names**: Standardizes team name formatting
- **Position Encoding**: Handles multi-position players (e.g., "MF,FW" → one-hot encoded columns)

#### 3. **Data Type Conversion**
- Numeric standardization: Converts string representations to appropriate numeric types
- Handles special cases: Age (Int64), Born (Int64), percentage fields (float)
- Removes non-numeric characters from numeric columns
- Imputes missing values with domain-appropriate defaults (e.g., 0 for defensive actions)

#### 4. **Normalization and Feature Engineering**
- **Per-90 Metrics**: Normalizes all counting statistics to per-90-minute rates for fair comparison
- **Duplicate Column Resolution**: Handles duplicate column names (e.g., "Tkl" for tackles vs. dribble tackles)
- **Feature Selection**: Removes low-variance features that don't contribute to analysis
- **Missing Data Handling**: Systematic imputation for goalkeepers (who lack shooting stats) and other edge cases

#### 5. **Data Integration**
- **Multi-Table Merging**: Combines defending, passing, and standard statistics on composite keys
- **Key Matching**: Uses player name, season, nation, position, squad, competition, and age for accurate joins
- **Suffix Management**: Handles overlapping column names with intelligent suffixing
- **Final Dataset**: Produces unified dataset with 95+ features per player-season observation

### Visualization and Exploratory Data Analysis

#### 1. **Correlation Analysis**
- **Heatmaps**: Comprehensive correlation matrices to identify relationships between features
- **Feature Relationships**: Visualizes dependencies between offensive, defensive, and passing metrics

#### 2. **Dimensionality Reduction Visualization**
- **Principal Component Analysis (PCA)**: Reduces 82+ features to 2D space for cluster visualization
- **Cluster Visualization**: Color-coded scatter plots showing player groupings in reduced space
- **Cluster Interpretation**: Identifies distinct player archetypes based on statistical profiles

#### 3. **Statistical Distributions**
- **Pair Plots**: Multi-variable relationships for key performance indicators
- **Distribution Analysis**: Understanding of metric distributions across positions and leagues

#### 4. **Cluster Profiling**
- **Mean Statistics by Cluster**: Identifies characteristic metrics for each player role
- **Position Distribution**: Analyzes how traditional positions map to statistical clusters
- **Seasonal Trends**: Tracks how player roles evolve over time

---

## Report Structure and Key Findings

### Dataset Structure

The final merged dataset (`merged_player_data.csv`) contains:

- **Dimensions**: ~14,000 player-season observations × 95+ features
- **Temporal Span**: 7 seasons (2017-2018 to 2023-2024)
- **Geographic Coverage**: 5 major European leagues
- **Player Filtering**: Minimum 5 "90s" (450 minutes) played per season

### Player Role Clustering

#### Methodology
- **Algorithm**: K-Means clustering with k=6 clusters
- **Feature Scaling**: StandardScaler (z-score normalization) applied to all numeric features
- **Feature Count**: 82 features after variance threshold filtering
- **Distance Metric**: Euclidean distance in standardized feature space

#### Identified Player Roles

Based on cluster analysis, the system identifies six distinct player archetypes:

1. **High-Volume Attacking Players** (Cluster 0)
   - Extremely high goals and assists per 90 minutes
   - Low defensive involvement
   - Typically forwards and attacking midfielders with limited minutes
   - Characterized by exceptional offensive output in limited playing time

2. **Balanced Midfielders/Defenders** (Cluster 1)
   - Moderate defensive actions (tackles, interceptions)
   - Balanced across all thirds of the field
   - Versatile players capable of contributing in multiple phases
   - Most common cluster, representing typical outfield players

3. **Goalkeepers** (Cluster 2)
   - Minimal offensive and passing statistics
   - Distinct statistical profile separate from outfield players
   - High minutes played (goalkeeper consistency)
   - Clearly separated in feature space

4. **High-Intensity Defenders** (Cluster 3)
   - Exceptional defensive metrics (tackles, interceptions, blocks)
   - High activity across all defensive actions
   - Typically center-backs and defensive midfielders
   - Characterized by defensive work rate and involvement

5. **Box-to-Box Midfielders** (Cluster 4)
   - Moderate defensive actions combined with offensive contributions
   - Higher progressive actions (carries, passes)
   - Balanced statistical profile indicating two-way play
   - Represents modern box-to-box midfielder archetype

6. **Limited-Minute Players** (Cluster 5)
   - Low overall statistical output
   - Very few minutes played
   - Typically substitutes or players with limited roles
   - Represents fringe squad members

### Key Analytical Insights

#### 1. **Positional Flexibility**
- The clustering reveals that traditional positions (DF, MF, FW) don't always align with statistical profiles
- Multi-position players (e.g., "MF,FW") often cluster with their more active role
- Defensive actions are the strongest differentiator between player types

#### 2. **Performance Normalization**
- Per-90 metrics enable fair comparison across players with varying minutes
- Clustering reveals that raw totals can be misleading; normalized metrics better capture playing style
- Goalkeepers form a distinct cluster, validating the need for position-specific analysis

#### 3. **Temporal Consistency**
- Player roles remain relatively stable across seasons within the same cluster
- Some players transition between clusters as their roles evolve
- The 7-season span enables tracking of player development and role changes

#### 4. **League Characteristics**
- All Big 5 leagues are represented across clusters
- No single league dominates any particular cluster, suggesting statistical profiles transcend league boundaries
- Wage data integration enables future analysis of value-to-performance relationships

### Output Deliverables

1. **Cleaned Datasets** (`cleaned_data/`):
   - `defending_cleaned.csv`: Defensive statistics with normalized metrics
   - `passing_cleaned.csv`: Passing statistics with distance-based breakdowns
   - `standard_cleaned.csv`: Standard player statistics with per-90 rates
   - `shooting_cleaned.csv`: Shooting efficiency metrics
   - `seasons_stats_cleaned.csv`: Squad-level aggregated statistics
   - `seasons_wages_cleaned.csv`: Wage data with multi-currency support
   - `merged_player_data.csv`: Final integrated dataset with cluster assignments

2. **Visualizations**:
   - PCA scatter plots showing cluster separation
   - Correlation heatmaps for feature relationships
   - Pair plots for key metric distributions
   - Cluster profile comparisons

3. **Analysis Reports**:
   - Cluster characteristics and interpretations
   - Player role classifications
   - Statistical profiles by position and cluster
   - Temporal trends across seasons

---

## Project Structure

```
scouting-report/
├── scraping.ipynb                 # Web scraping pipeline
├── data_cleaning.ipynb            # Data cleaning and transformation
├── EDA.ipynb                      # Exploratory data analysis
├── player_role_clustering.ipynb   # K-means clustering and role identification
├── requirements.txt               # Python dependencies
├── data_html/                     # Raw HTML scraped data
├── uncleaned_data_csv/            # Initial CSV exports
├── cleaned_data/                  # Processed and cleaned datasets
└── src/                          # Utility modules
    └── fbref_utils.py            # Common cleaning functions
```

---

## Technical Highlights

### Robust Error Handling
- Graceful handling of missing data, duplicate headers, and inconsistent formats
- Systematic imputation strategies for position-specific missing values
- Validation checks to ensure data integrity throughout the pipeline

### Scalable Architecture
- Modular functions for reusable data processing steps
- Separation of concerns: scraping, cleaning, analysis, and visualization
- Configurable parameters for different analysis scenarios

### Production-Ready Data Quality
- Comprehensive data validation at each processing stage
- Handling of edge cases (goalkeepers, multi-position players, limited minutes)
- Consistent naming conventions and data types across all outputs

---

## Future Enhancements

- **Advanced Clustering**: Hierarchical clustering or DBSCAN for more nuanced role identification
- **Time Series Analysis**: Tracking player development and role evolution over multiple seasons
- **Comparative Reports**: Automated generation of player comparison reports
- **Predictive Elements**: Integration with TOTY Predictor project for performance forecasting
- **Interactive Dashboards**: Web-based visualization tools for dynamic exploration

---

## Dependencies

See `requirements.txt` for complete list. Key dependencies:
- requests
- beautifulsoup4
- pandas
- numpy
- scikit-learn
- matplotlib
- seaborn
- fuzzywuzzy
- python-Levenshtein
- selenium (for dynamic content)

---

## Development History

This project evolved through five distinct development phases, each building upon previous work to create a comprehensive football analytics platform.

### Phase 1: Initial Data Acquisition (August 2024)
**Duration**: August 23-27, 2024

The project began with establishing the foundational data collection infrastructure. Initial commits focused on successfully scraping squad-level statistics and wage data from FBref.com for the Big 5 European leagues. The early implementation laid the groundwork for multi-source data collection, with initial success in extracting team statistics and wage information across multiple currencies. This phase established the core web scraping architecture that would later be expanded to include player-level statistics.

### Phase 2: Core Data Cleaning Pipeline (May-June 2025)
**Duration**: May 26 - June 15, 2025

The most intensive development phase focused on building a robust, modular data cleaning pipeline. Development efforts centered on systematically processing defending, passing, and standard statistics datasets, with particular attention to handling inconsistent headers, duplicate columns, and data type conversions. Key technical achievements included implementing normalization functions for per-90 metrics, creating modular cleaning functions for reusability, and resolving complex issues such as nation column standardization and header row identification. The pipeline was designed with reproducibility in mind, ensuring consistent data quality across all processed datasets.

### Phase 3: Advanced Analysis and Machine Learning (June 2025)
**Duration**: June 16-27, 2025

This phase transitioned from data preparation to analytical insights through the implementation of unsupervised learning techniques. The development team implemented K-means clustering for player role classification, addressing challenges related to dataset size and feature selection. Critical work included resolving data loss issues during merging operations, implementing proper feature scaling, and optimizing the clustering pipeline to handle the full dataset without lazy imputation. The phase culminated in successful player classification with distinct role identification, validating the data preparation work from Phase 2.

### Phase 4: System Refinement and Data Expansion (July-September 2025)
**Duration**: July 1 - September 15, 2025

The refinement phase focused on expanding data coverage and improving system architecture. Major additions included integrating shooting statistics using Selenium for dynamic content scraping, implementing one-hot encoding for position data to support both modeling and visualization needs, and preparing the dataset for external analytics platforms (Google BigQuery). Technical improvements included column renaming for database compatibility, creating utility functions to standardize cleaning processes across all data types, and finalizing the complete data processing pipeline. This phase transformed the project from a collection of scripts into a cohesive, production-ready system.

### Phase 5: Documentation and Finalization (November-December 2025)
**Duration**: November 5 - December 12, 2025

The final phase focused on professional documentation and system completion. Development efforts included finalizing the seasons_wages cleaning process, creating utility modules for standardized cleaning workflows, and comprehensive README documentation. The project was prepared for professional presentation, with clear documentation of technical achievements, methodology, and outcomes suitable for technical hiring managers and stakeholders. This phase ensured the project's reproducibility and demonstrated the full scope of technical capabilities across the entire development lifecycle.

---

## Usage

1. **Data Collection**: Run `scraping.ipynb` to collect data from FBref.com
2. **Data Cleaning**: Execute `data_cleaning.ipynb` to process and clean raw data
3. **Exploratory Analysis**: Use `EDA.ipynb` for initial data exploration
4. **Clustering**: Run `player_role_clustering.ipynb` to generate player role classifications
5. **Analysis**: Use cleaned datasets in `cleaned_data/` for custom analysis

---

*This project demonstrates expertise in data engineering, statistical analysis, machine learning, and domain-specific analytics for professional football scouting and player evaluation.*
