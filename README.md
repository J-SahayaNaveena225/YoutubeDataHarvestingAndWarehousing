# YouTube Data Harvesting and Warehousing

## Overview
This project gathers data from YouTube channels using the YouTube Data API, processes it, and stores it in a SQL data warehouse. The application is built using Streamlit, allowing users to enter a YouTube channel ID to fetch and store data. It also provides search capabilities to retrieve data from the SQL database.

## Features
- Fetch channel details, video IDs, video data, playlist details, and comment data from YouTube using the YouTube Data API.
- Store the retrieved data in a MySQL database.
- Provide a Streamlit interface for users to interact with the application.
- Answer predefined questions by querying the SQL database.

## Technologies Used
- **Python**: The main programming language.
- **Google API Client**: To connect to the YouTube Data API.
- **MySQL**: For data storage.
- **Pandas**: For data manipulation and display.
- **Streamlit**: For creating the web interface.
- **Streamlit Option Menu**: For creating a sidebar menu.

## Setup Instructions
### Prerequisites
- Python 3.6 or later
- MySQL server
- Required Python libraries: googleapiclient, mysql-connector-python, pandas, streamlit, streamlit-option-menu

### Installation
1. **Clone the repository**:
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```

2. **Install the required libraries**:
   ```bash
   pip install google-api-python-client mysql-connector-python pandas streamlit streamlit-option-menu
   ```

3. **Setup MySQL Database**:
   - Start your MySQL server.
   - Create a database named `Youtubedataharvestingandwarehousing`.
   - Update the connection details (host, user, password, database, port) in the script if necessary.

4. **Run the Streamlit application**:
   ```bash
   streamlit run app.py
   ```

## Application Structure
### API Connection
Connects to the YouTube Data API using the provided API key.

### Fetching Data
Functions to fetch channel details, video IDs, video data, playlist details, and comment data from YouTube.

### Storing Data
Functions to create tables and store the fetched data in a MySQL database.

### Streamlit Interface
- **Home**: Overview of the project.
- **Fetch & Store**: Input a YouTube channel ID to fetch and store data.
- **Q/A**: Select and execute predefined queries to retrieve data from the database.

## Usage
1. Run the Streamlit application.
2. Navigate to the "Fetch & Store" menu to input a YouTube channel ID and store data.
3. Use the "Q/A" menu to query and display data from the database.

## License
This project is licensed under the MIT License.

## Contact
For any questions or suggestions, feel free to contact [your-email@example.com].