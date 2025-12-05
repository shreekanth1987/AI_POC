import streamlit as st
import json
import pymongo
from datetime import datetime
from typing import List, Dict, Any
import networkx as nx
from streamlit_agraph import agraph, Node, Edge, Config
from collections import defaultdict

# Page config
st.set_page_config(page_title="OpenLineage Viewer", layout="wide", height=800)
st.title("🔗 OpenLineage Data Lineage Viewer")

# ✅ FIXED: Use st.cache_resource for MongoDB connections
@st.cache_resource
def get_mongo_client(_uri: str):
    """Cached MongoDB client - URI as key to support dynamic connections"""
    return pymongo.MongoClient(
        _uri, 
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000
    )

@st.cache_data(ttl=300)  # Cache events for 5 minutes
def load_events(_client, _db_name: str, _coll: str, limit: int):
    """Query latest OpenLineage event per job - now fully cacheable"""
    db = _client[_db_name]
    pipeline = [
        {"$sort": {"job.namespace": 1, "job.name": 1, "eventTime": -1}},
        {"$group": {
            "_id": {"namespace": "$job.namespace", "name": "$job.name"},
            "latestEvent": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$latestEvent"}},
        {"$limit": limit}
    ]
    return list(db[_coll].aggregate(pipeline))

def parse_lineage_graph(events: List[Dict]) -> tuple:
    """Parse OpenLineage events into nodes and edges for graph"""
    nodes_data = []
    edges = []
    
    job_count = 0
    dataset_count = 0
    
    for event in events:
        if event.get("eventType") == "COMPLETE" and event.get("job"):
            # Job node (blue)
            job_namespace = event["job"].get("namespace", "unknown")
            job_name = event["job"].get("name", "unknown")
            job_id = f"{job_namespace}.{job_name}"
            
            nodes_data.append({
                "id": job_id,
                "label": job_name,
                "namespace": job_namespace,
                "type": "job",
                "color": "#1f77b4",
                "size": 25
            })
            job_count += 1
            
            # Inputs (upstream datasets - orange)
            for inp in event.get("inputs", []):
                ds_namespace = inp.get("namespace", "unknown")
                ds_name = inp.get("name", "unknown")
                ds_id = f"{ds_namespace}.{ds_name}"
                
                nodes_data.append({
                    "id": ds_id,
                    "label": ds_name,
                    "namespace": ds_namespace,
                    "type": "dataset",
                    "color": "#ff7f0e",
                    "size": 20
                })
                dataset_count += 1
                
                edges.append(Edge(source=ds_id, target=job_id, label="input"))
            
            # Outputs (downstream datasets - green)
            for outp in event.get("outputs", []):
                ds_namespace = outp.get("namespace", "unknown")
                ds_name = outp.get("name", "unknown")
                ds_id = f"{ds_namespace}.{ds_name}"
                
                nodes_data.append({
                    "id": ds_id,
                    "label": ds_name,
                    "namespace": ds_namespace,
                    "type": "dataset",
                    "color": "#2ca02c",
                    "size": 20
                })
                dataset_count += 1
                
                edges.append(Edge(source=job_id, target=ds_id, label="output"))
    
    return nodes_data, edges, job_count, dataset_count

# Sidebar configuration
st.sidebar.header("🔌 MongoDB Connection")
mongo_uri = st.sidebar.text_input(
    "MongoDB URI", 
    value="mongodb://localhost:27017/",
    help="e.g., mongodb://user:pass@host:27017/"
)
db_name = st.sidebar.text_input("Database", value="openlineage")
collection = st.sidebar.text_input("Collection", value="openlineage_events")
job_limit = st.sidebar.slider("Max jobs to load", 10, 500, 100)

st.sidebar.header("⚙️ Filters")
namespace_filter = st.sidebar.multiselect(
    "Filter namespaces", 
    options=[],
    help="Filter will be available after data loads"
)

if st.sidebar.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

# Get MongoDB client (cached by URI)
try:
    client = get_mongo_client(mongo_uri)
    st.sidebar.success("✅ Connected to MongoDB")
except Exception as e:
    st.sidebar.error(f"❌ MongoDB Error: {str(e)}")
    st.sidebar.info("💡 Check URI, auth, and network")

# Main tabs
tab1, tab2, tab3 = st.tabs(["📊 Lineage Graph", "📋 Latest Events", "ℹ️ Info"])

with tab1:
    if 'client' in locals():
        with st.spinner("Loading lineage events..."):
            events = load_events(client, db_name, collection, job_limit)
        
        if events:
            nodes_data, edges, job_count, dataset_count = parse_lineage_graph(events)
            
            # Apply namespace filter
            if namespace_filter:
                nodes_data = [n for n in nodes_data if n["namespace"] in namespace_filter]
                edges = [e for e in edges if any(n["namespace"] in namespace_filter 
                                               for n in nodes_data 
                                               if n["id"] == e.source or n["id"] == e.target)]
            
            if nodes_data:
                config = Config(
                    width=1400,
                    height=900,
                    directed=True,
                    physics=True,
                    nodeHighlightBehavior=True,
                    highlightColor="#F7A7A6",
                    collapsible=True,
                    nodesFilter="type",
                    hierarchical=False
                )
                
                # Convert to agraph format
                nodes = [Node(
                    id=n["id"], 
                    label=n["label"], 
                    size=n["size"], 
                    color=n["color"],
                    shape="box",
                    title=f"Type: {n['type']}\nNamespace: {n['namespace']}"
                ) for n in nodes_data]
                
                st_agraph = agraph(nodes=nodes, edges=edges, config=config)
                
                # Metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1: st.metric("Jobs", job_count)
                with col2: st.metric("Datasets", dataset_count)
                with col3: st.metric("Nodes", len(nodes_data))
                with col4: st.metric("Edges", len(edges))
                
                # Update namespace filter options
                unique_ns = list(set(n["namespace"] for n in nodes_data))
                if unique_ns != namespace_filter:
                    st.cache_data.clear()
                
            else:
                st.warning("No lineage data found matching filters.")
        else:
            st.info("👆 No events found. Check your MongoDB collection has COMPLETE events with job/inputs/outputs.")
    else:
        st.warning("Connect to MongoDB first (sidebar)")

with tab2:
    if 'client' in locals() and 'events' in locals():
        st.dataframe(
            events, 
            use_container_width=True,
            column_config={
                "eventTime": st.column_config.DateColumn("Timestamp"),
                "eventType": st.column_config.SelectboxColumn("Type")
            }
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 Export JSON"):
                st.download_button(
                    label="Download Events",
                    data=json.dumps(events, indent=2, default=str),
                    file_name=f"openlineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        with col2:
            st.json({"sample_event_keys": list(events[0].keys()) if events else []})
    else:
        st.info("Load data in Graph tab first")

with tab3:
    st.markdown("""
    ## 🛠️ MongoDB Query Used
    
    ```
    db.openlineage_events.aggregate([
      {"$sort": {"job.namespace": 1, "job.name": 1, "eventTime": -1}},
      {"$group": {
        "_id": {"namespace": "$job.namespace", "name": "$job.name"},
        "latestEvent": {"$first": "$$ROOT"}
      }},
      {"$replaceRoot": {"newRoot": "$latestEvent"}},
      {"$limit": 100}
    ])
    ```
    
    ## 🚀 Features
    - **Interactive graph** with drag/zoom (streamlit-agraph)
    - **Latest event per job** (MongoDB aggregation)
    - **Color coding**: Jobs=🔵, Inputs=🟠, Outputs=🟢
    - **Namespace filtering** & real-time refresh
    - **Export** raw JSON events
    
    ## 📦 Install
    ```
    pip install streamlit pymongo streamlit-agraph networkx
    streamlit run lineage_viewer.py
    ```
    """)

# Footer
st.markdown("---")
st.markdown("*Built for OpenLineage → MongoDB → Atlan-style visualization* 👨‍💻")
