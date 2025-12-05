import streamlit as st
import json
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
from loadMongoDB import load_config, retrieve_and_print_json_getlatest
import tempfile

# Page configuration
st.set_page_config(page_title="Data Lineage Viewer", page_icon="🔗", layout="wide")

# Custom CSS
st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: 700; color: #1F2937; margin-bottom: 0.5rem; }
    .sub-header { font-size: 1.2rem; color: #6B7280; margin-bottom: 2rem; }
    .bronze-badge { background-color: #CD7F32; color: white; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.875rem; font-weight: 600; }
    .silver-badge { background-color: #C0C0C0; color: white; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.875rem; font-weight: 600; }
    .gold-badge { background-color: #FFD700; color: #1F2937; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.875rem; font-weight: 600; }
    .source-badge { background-color: #10B981; color: white; padding: 0.25rem 0.75rem; border-radius: 1rem; font-size: 0.875rem; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<div class="main-header">🔗 Data Lineage Explorer</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Interactive medallion architecture visualization</div>', unsafe_allow_html=True)

@st.cache_data
def load_lineage_data(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
    
if 'data' not in st.session_state:
    CONFIG_FILE = "utils\mongoConfig.JSON"
    config = load_config(CONFIG_FILE)
    if config:
        try:
            MONGODB_URI = config["MONGODB_URI"]
            DATABASE_NAME = config["DATABASE_NAME"]
            COLLECTION_NAME = config["COLLECTION_NAME"]
            print("\n--- Starting MongoDB RETRIEVAL (Latest Completed Jobs) ---")
            data = retrieve_and_print_json_getlatest(MONGODB_URI, DATABASE_NAME, COLLECTION_NAME)
            st.session_state["data"] = {
                "description": "OpenLineage events for medallion architecture with complete schema information",
                "events":data
                }
        except KeyError as e:
            print(f"Error: Missing required key '{e}' in {CONFIG_FILE}. Check your configuration file.")


def determine_layer(name):
    name_lower = name.lower()
    if 'bronze' in name_lower or 'raw' in name_lower: return 'bronze'
    elif 'silver' in name_lower or 'clean' in name_lower: return 'silver'
    elif 'gold' in name_lower or 'analytics' in name_lower: return 'gold'
    elif any(x in name_lower for x in ['postgres', 'sqlserver', 'mongodb', 'public.', 'dbo.', '.csv']): return 'source'
    return 'unknown'

def extract_source_system(namespace):
    if 'postgres' in namespace: return 'PostgreSQL'
    elif 'sqlserver' in namespace: return 'SQL Server'
    elif 'mongodb' in namespace: return 'MongoDB'
    elif 'file://' in namespace: return 'CSV Files'
    elif 's3://' in namespace: return 'Data Lake'
    return 'Unknown'

def get_source_table_name(namespace, name):
    if namespace.startswith('s3://'): return name
    if '.' in name: return name.split('.')[-1]
    return name

def get_node_color(layer):
    colors = {'source': '#10B981', 'bronze': '#CD7F32', 'silver': '#C0C0C0', 'gold': '#FFD700', 'unknown': '#6B7280'}
    return colors.get(layer, '#6B7280')

def get_node_shape(layer):
    shapes = {'source': 'database', 'bronze': 'box', 'silver': 'box', 'gold': 'box', 'unknown': 'ellipse'}
    return shapes.get(layer, 'ellipse')

def extract_lineage_graph(lineage_data):
    nodes = {}
    edges = []

    for event in lineage_data.get('events', []):
        job_name = event.get('job', {}).get('name', 'Unknown Job')
        job_namespace = event.get('job', {}).get('namespace', '')

        job_id = f"job_{job_name}"
        if job_id not in nodes:
            nodes[job_id] = {
                'id': job_id, 'label': job_name, 'type': 'job', 'layer': 'job',
                'source_system': 'Databricks', 'schema_fields': [],
                'details': {'namespace': job_namespace, 'event_time': event.get('eventTime', 'N/A')}
            }

        for input_ds in event.get('inputs', []):
            ds_namespace = input_ds.get('namespace', '')
            ds_name = input_ds.get('name', '')
            ds_id = f"{ds_namespace}/{ds_name}"
            schema_fields = input_ds.get('facets', {}).get('schema', {}).get('fields', [])

            if ds_id not in nodes:
                nodes[ds_id] = {
                    'id': ds_id, 'label': ds_name, 'type': 'dataset',
                    'layer': determine_layer(ds_name),
                    'source_system': extract_source_system(ds_namespace),
                    'table_name': get_source_table_name(ds_namespace, ds_name),
                    'schema_fields': schema_fields if schema_fields else [],
                    'details': {'namespace': ds_namespace, 'full_name': ds_id, 'row_count': 'N/A', 'size': 'N/A'}
                }
            elif not nodes[ds_id].get('schema_fields') and schema_fields:
                nodes[ds_id]['schema_fields'] = schema_fields

            edges.append((ds_id, job_id))

        for output_ds in event.get('outputs', []):
            ds_namespace = output_ds.get('namespace', '')
            ds_name = output_ds.get('name', '')
            ds_id = f"{ds_namespace}/{ds_name}"
            schema_fields = output_ds.get('facets', {}).get('schema', {}).get('fields', [])
            output_stats = output_ds.get('facets', {}).get('outputStatistics', {})

            if ds_id not in nodes:
                nodes[ds_id] = {
                    'id': ds_id, 'label': ds_name, 'type': 'dataset',
                    'layer': determine_layer(ds_name),
                    'source_system': extract_source_system(ds_namespace),
                    'table_name': get_source_table_name(ds_namespace, ds_name),
                    'schema_fields': schema_fields if schema_fields else [],
                    'details': {
                        'namespace': ds_namespace, 'full_name': ds_id,
                        'row_count': output_stats.get('rowCount', 'N/A'),
                        'size': output_stats.get('size', 'N/A')
                    }
                }
            else:
                nodes[ds_id]['details']['row_count'] = output_stats.get('rowCount', nodes[ds_id]['details'].get('row_count', 'N/A'))
                nodes[ds_id]['details']['size'] = output_stats.get('size', nodes[ds_id]['details'].get('size', 'N/A'))
                if not nodes[ds_id].get('schema_fields') and schema_fields:
                    nodes[ds_id]['schema_fields'] = schema_fields

            edges.append((job_id, ds_id))

    return nodes, edges

def get_lineage_subgraph(nodes, edges, selected_table):
    if not selected_table or selected_table == "All Tables":
        return nodes, edges

    G = nx.DiGraph()
    G.add_edges_from(edges)

    target_node_id = None
    for node_id, node_data in nodes.items():
        if node_data['type'] == 'dataset' and node_data['table_name'] == selected_table:
            target_node_id = node_id
            break

    if not target_node_id:
        return nodes, edges

    lineage_nodes = set([target_node_id])
    try:
        lineage_nodes.update(nx.ancestors(G, target_node_id))
        lineage_nodes.update(nx.descendants(G, target_node_id))
    except: pass

    filtered_nodes = {k: v for k, v in nodes.items() if k in lineage_nodes}
    filtered_edges = [(s, t) for s, t in edges if s in lineage_nodes and t in lineage_nodes]

    return filtered_nodes, filtered_edges

def create_tooltip_html(node_data):
    """Create HTML tooltip with proper formatting"""
    if node_data['type'] == 'job':
        html = f"""<div style='font-family: Arial; font-size: 12px;'>
<b>Job: {node_data['label']}</b><br/>
Namespace: {node_data['details']['namespace']}<br/>
Event Time: {node_data['details']['event_time']}
</div>"""
        return html
    else:
        layer = node_data['layer']
        html = f"""<div style='font-family: Arial; font-size: 12px; max-width: 400px;'>
<b style='font-size: 14px;'>{node_data['label']}</b><br/>
<span style='color: #666;'>Layer: {layer.upper()}</span><br/>
<span style='color: #666;'>Source: {node_data['source_system']}</span><br/>"""

        if 'row_count' in node_data['details'] and node_data['details']['row_count'] != 'N/A':
            html += f"<span style='color: #666;'>Rows: {node_data['details']['row_count']:,}</span><br/>"

        if 'size' in node_data['details'] and node_data['details']['size'] != 'N/A':
            size_mb = node_data['details']['size'] / (1024 * 1024)
            html += f"<span style='color: #666;'>Size: {size_mb:.2f} MB</span><br/>"

        schema_fields = node_data.get('schema_fields', [])
        if schema_fields and len(schema_fields) > 0:
            html += f"""<br/>
<b style='color: #2563EB;'>Schema ({len(schema_fields)} columns):</b><br/>
<div style='font-family: monospace; font-size: 11px; margin-left: 10px;'>"""

            for field in schema_fields[:10]:
                col_name = field.get('name', 'N/A')
                col_type = field.get('type', 'N/A')
                html += f"• {col_name} <span style='color: #059669;'>({col_type})</span><br/>"

            if len(schema_fields) > 10:
                html += f"<span style='color: #666;'>... and {len(schema_fields) - 10} more columns</span><br/>"

            html += "</div>"

        html += "</div>"
        return html

def create_interactive_graph(nodes, edges, show_jobs=True):
    net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="#1F2937", directed=True)

    net.set_options("""
    {
        "physics": {"enabled": true, "hierarchicalRepulsion": {"centralGravity": 0.3, "springLength": 200, "springConstant": 0.01, "nodeDistance": 250, "damping": 0.09}, "solver": "hierarchicalRepulsion"},
        "layout": {"hierarchical": {"enabled": true, "direction": "LR", "sortMethod": "directed", "levelSeparation": 300, "nodeSpacing": 200}},
        "edges": {"color": {"color": "#9CA3AF", "highlight": "#3B82F6"}, "smooth": {"enabled": true, "type": "cubicBezier"}, "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}}},
        "interaction": {"hover": true, "navigationButtons": true, "keyboard": true, "tooltipDelay": 100}
    }
    """)

    for node_id, node_data in nodes.items():
        if node_data['type'] == 'job' and not show_jobs:
            continue

        if node_data['type'] == 'job':
            color, shape, size = '#6366F1', 'diamond', 20
        else:
            color = get_node_color(node_data['layer'])
            shape = get_node_shape(node_data['layer'])
            size = 25

        tooltip = create_tooltip_html(node_data)

        net.add_node(node_id, label=node_data['label'], title=tooltip, color=color, shape=shape, size=size)

    for source, target in edges:
        if not show_jobs and ('job_' in source or 'job_' in target):
            continue
        net.add_edge(source, target, width=2)

    if not show_jobs:
        for source, target in edges:
            if 'job_' in target:
                for s2, t2 in edges:
                    if s2 == target and 'job_' not in t2:
                        net.add_edge(source, t2, width=2)

    return net

# Main app
try:
    # lineage_data = load_lineage_data('openlineage_medallion_architecture.json')
    all_nodes, all_edges = extract_lineage_graph(st.session_state["data"])

    source_systems = set()
    source_tables = set()
    for node_id, node_data in all_nodes.items():
        if node_data['type'] == 'dataset':
            source_systems.add(node_data['source_system'])
            source_tables.add(node_data['table_name'])

    st.sidebar.header("🎯 Filters")
    selected_source_system = st.sidebar.selectbox("Source System", ["All Systems"] + sorted(list(source_systems)))

    if selected_source_system != "All Systems":
        available_tables = set()
        for node_id, node_data in all_nodes.items():
            if node_data['type'] == 'dataset' and node_data['source_system'] == selected_source_system:
                available_tables.add(node_data['table_name'])
        filtered_tables = sorted(list(available_tables))
    else:
        filtered_tables = sorted(list(source_tables))

    selected_table = st.sidebar.selectbox("Table / Dataset", ["All Tables"] + filtered_tables)
    st.sidebar.markdown("---")
    show_jobs = st.sidebar.checkbox("Show Job Nodes", value=True)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Legend")
    st.sidebar.markdown('<span class="source-badge">SOURCE</span> Source Systems', unsafe_allow_html=True)
    st.sidebar.markdown('<span class="bronze-badge">BRONZE</span> Raw Data', unsafe_allow_html=True)
    st.sidebar.markdown('<span class="silver-badge">SILVER</span> Cleaned Data', unsafe_allow_html=True)
    st.sidebar.markdown('<span class="gold-badge">GOLD</span> Analytics', unsafe_allow_html=True)

    if selected_source_system != "All Systems":
        filtered_nodes = {k: v for k, v in all_nodes.items() if v['type'] == 'job' or v['source_system'] == selected_source_system}
        filtered_edges = [(s, t) for s, t in all_edges if s in filtered_nodes and t in filtered_nodes]
    else:
        filtered_nodes, filtered_edges = all_nodes, all_edges

    nodes, edges = get_lineage_subgraph(filtered_nodes, filtered_edges, selected_table)

    dataset_nodes = [n for n in nodes.values() if n['type'] == 'dataset']
    job_nodes = [n for n in nodes.values() if n['type'] == 'job']

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📊 Datasets", len(dataset_nodes))
    col2.metric("⚙️ Jobs", len(job_nodes))
    col3.metric("🔗 Connections", len(edges))
    col4.metric("🏗️ Layers", len(set(n['layer'] for n in dataset_nodes)))

    st.markdown("---")

    if selected_table != "All Tables":
        st.info(f"📌 Showing lineage for: **{selected_table}**")

    tab1, tab2 = st.tabs(["📈 Lineage Graph", "📋 Dataset Details"])

    with tab1:
        st.markdown("### Interactive Lineage Graph")
        st.info("💡 **Hover over nodes** to see schema details including column names and types. Drag to rearrange, scroll to zoom.")

        if len(nodes) == 0:
            st.warning("No data matches the selected filters.")
        else:
            net = create_interactive_graph(nodes, edges, show_jobs)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as f:
                net.save_graph(f.name)
                with open(f.name, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                components.html(html_content, height=650, scrolling=True)

    with tab2:
        st.markdown("### Dataset Catalog")

        if len(dataset_nodes) == 0:
            st.warning("No datasets match the selected filters.")
        else:
            layer_filter = st.selectbox("Filter by Layer", ["All"] + sorted(list(set(n['layer'] for n in dataset_nodes))))
            filtered_datasets = [n for n in dataset_nodes if layer_filter == "All" or n['layer'] == layer_filter]

            for ds in filtered_datasets:
                with st.expander(f"🗂️ {ds['label']} ({ds['layer'].upper()}) - {len(ds.get('schema_fields', []))} columns"):
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        st.markdown(f"**Table:** {ds['table_name']}")
                        st.markdown(f"**Source:** {ds['source_system']}")
                        st.markdown(f"**Path:** `{ds['details']['full_name']}`")
                        if 'row_count' in ds['details'] and ds['details']['row_count'] != 'N/A':
                            st.markdown(f"**Rows:** {ds['details']['row_count']:,}")
                        if 'size' in ds['details'] and ds['details']['size'] != 'N/A':
                            st.markdown(f"**Size:** {ds['details']['size'] / (1024*1024):.2f} MB")

                    with col2:
                        layer = ds['layer']
                        if layer == 'bronze': st.markdown('<span class="bronze-badge">BRONZE</span>', unsafe_allow_html=True)
                        elif layer == 'silver': st.markdown('<span class="silver-badge">SILVER</span>', unsafe_allow_html=True)
                        elif layer == 'gold': st.markdown('<span class="gold-badge">GOLD</span>', unsafe_allow_html=True)
                        elif layer == 'source': st.markdown('<span class="source-badge">SOURCE</span>', unsafe_allow_html=True)

                    schema_fields = ds.get('schema_fields', [])
                    if schema_fields:
                        st.markdown(f"**Schema ({len(schema_fields)} columns):**")
                        schema_data = [{'Column': f['name'], 'Type': f['type'], 'Description': f.get('description', '-')} for f in schema_fields]
                        st.table(schema_data)

except FileNotFoundError:
    st.error("⚠️ File 'openlineage_medallion_architecture.json' not found.")
except Exception as e:
    st.error(f"⚠️ Error: {str(e)}")
    st.exception(e)

st.markdown("---")
st.markdown("🔗 Built with Streamlit • Data Lineage powered by OpenLineage")