import streamlit as st

# Your Python dictionary or list of dictionaries
my_json_data = {
    "name": "Streamlit App Config",
    "version": "1.0",
    "status": "running",
    "components": [
        {"id": 1, "type": "selectbox", "options": ["A", "B"]},
        {"id": 2, "type": "dataframe", "data_shape": [10, 5]}
    ],
    "nested_config": {
        "level1": {
            "level2": {"setting": True, "value": 42}
        }
    }
}

st.json(my_json_data, expanded=True)
# You can set expanded=False to collapse the entire tree by default,
# or set expanded=1, 2, etc., to specify the initial expansion depth.