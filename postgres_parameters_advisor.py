import streamlit as st
import pandas as pd
import json
import re
import os
from datetime import datetime

# Title
st.markdown("""
    <style>
    h1, h2, h3, h4, h5, h6 {
        text-align: center;
    }
    img {
        display: block;
        margin-left: auto;
        margin-right: auto;
    }
    </style>
    """, unsafe_allow_html=True)

st.warning('This tool is under testing. Use it at your own risk!', icon="⚠️")

# Custom header with logo left of title
st.markdown(
    """<div style="display: flex; align-items: center; gap: 20px; margin-bottom: 10px;">
        <img src="https://azure.microsoft.com/svghandler/postgresql?width=100" style="height:60px; width:auto;" alt="PostgreSQL Logo" />
        <h1 style="margin:0;">Parameters Advisor for Azure PostgreSQL Flex Server</h1>
    </div>""", unsafe_allow_html=True)


# Sidebar inputs
st.sidebar.markdown('<img src="https://swimburger.net/media/ppnn3pcl/azure.png" style="height:40px; display:block; margin-left:auto; margin-right:auto;" alt="Azure" />', unsafe_allow_html=True)
st.sidebar.header("Input Configuration")

support_ticket = st.sidebar.text_input("Support Ticket ID")
email = st.sidebar.text_input("Email Address")

def is_valid_email(email):
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, email)

inputs_enabled = bool(support_ticket.strip()) and bool(email.strip()) and is_valid_email(email)

if not bool(support_ticket.strip()):
    st.sidebar.warning("Support Ticket ID is required.")
if not bool(email.strip()):
    st.sidebar.warning("Email address is required.")
elif email and not is_valid_email(email):
    st.sidebar.warning("Please enter a valid email address.")

db_role = st.sidebar.selectbox("Database Role", ["OLTP", "OLAP", "RAG", "Mixed"], disabled=not inputs_enabled)
pg_version = st.sidebar.selectbox("PostgreSQL Version", ["17", "16", "15", "14", "13", "12"], disabled=not inputs_enabled)
server_cpus = st.sidebar.selectbox("CPUs", [1,2,4,8,16,20,32,48,64,96,128,192], disabled=not inputs_enabled)
memory_gb = st.sidebar.selectbox("Memory (GB)", [2,4,8,16,32,48,64,80,96,128,160,192,256,384,432,512,672,768,1024,1832], disabled=not inputs_enabled)

#max_connections = st.sidebar.number_input("Max Connections", min_value=10, max_value=10000, value=100) # Shash said the default is 5k max and should be left as it is
#server_tier = st.sidebar.selectbox("Server Tier", ["General Purpose", "Memory Optimized", "Compute Optimized"])
#server_class = st.sidebar.selectbox(
#    "Server Class",
#    ["B1ms", "B2s", "D2s", "D4s", "D8s", "D16s", "D32s", "D48s", "D64s",
#     "D2ds", "D4ds", "D8ds", "D16ds", "D32ds", "D48ds", "D64ds",
#     "E2s", "E4s", "E8s", "E16s", "E32s", "E48s", "E64s",
#     "E2ds", "E4ds", "E8ds", "E16ds", "E20ds", "E32ds", "E48ds", "E64ds"]
#)

# Extract numeric portion from server_class
#server_cpus_match = re.search(r'\d+', server_class)
#server_cpus = st.sidebar.selectbox("Max CPUs", int(server_cpus_match.group()) if server_cpus_match else 0)
#storage_type = st.sidebar.selectbox("Storage Type", ["SSD", "SSD_v2", "SSD_ultra"]) # commenting it out as not being used to adjust any parameter
#storage_size = st.sidebar.number_input("Storage Size (GB)", min_value=10, max_value=65536, value=100) # Irrelevant for now...
#storage_iops = st.sidebar.selectbox(
#    "Storage IOPS",
#    [
#        "120", "240", "500", "1100", "2300", "3200", "5000", "6400", "7500",
#        "12800", "16000", "18000", "20000", "25600", "32000", "51200", "76800", "80000"
#    ]
#)
#db_size = st.sidebar.number_input("Database Size (GB)", min_value=1, max_value=65536, value=50)

def get_recommendations(memory, role):
    base = {
        "shared_buffers": int(memory * 1024 * 0.25),
        #"work_mem": int(memory * 1024 / max_connections), # Remove dependency of max_connections
        #"maintenance_work_mem": int(memory * 1024 * 0.1),
        "effective_cache_size": int(memory * 1024 * 0.75),
        "random_page_cost": 1,
        "default_statistics_target": 100,
        "from_collapse_limit": 40,
        "join_collapse_limit": 40,
        "max_parallel_workers": (8 if server_cpus <= 16 else server_cpus / 2),
        "max_worker_processes": (8 if server_cpus <= 16 else server_cpus / 2),
        "max_parallel_workers_per_gather": (2 if server_cpus <= 8 else server_cpus / 4),
        "max_parallel_maintenance_workers": (2 if server_cpus <= 8 else server_cpus / 4),
    }

    if role == "OLTP":
        factor_general = {"conservative": 0.8, "balanced": 1.0, "aggressive": 1.2}
        factor_random_page_cost = {"conservative": 1.2, "balanced": 1.1, "aggressive": 1.08}
        factor_default_statistics_target = {"conservative": 5, "balanced": 20, "aggressive": 50}
        factor_collapse_limits = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        work_mem_setting = {"conservative": 16, "balanced": 32, "aggressive": 64}
    elif role == "OLAP":
        factor_general = {"conservative": 1.0, "balanced": 1.2, "aggressive": 1.5}
        factor_random_page_cost = {"conservative": 1.1, "balanced": 1.08, "aggressive": 1.05} # Favours more full scans than using indexes
        factor_default_statistics_target = {"conservative": 5, "balanced": 10, "aggressive": 30}
        factor_collapse_limits = {"conservative": 1.2, "balanced": 1.5, "aggressive": 2} # Assuming OLAP will have larger queries with more JOINs
        factor_shared_buffers = {"conservative": 1, "balanced": 1.25, "aggressive": 1.6}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128} 
    elif role == "RAG": # For RAG basically increase memory
        factor_general = {"conservative": 1, "balanced": 1.25, "aggressive": 1.5}
        factor_random_page_cost = {"conservative": 1.15, "balanced": 1.1, "aggressive": 1.1}
        factor_default_statistics_target = {"conservative": 10, "balanced": 20, "aggressive": 50}
        factor_collapse_limits = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.25, "aggressive": 1.6}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128} 
    else:  # Mixed
        factor_general = {"conservative": 0.9, "balanced": 1.1, "aggressive": 1.3}
        factor_random_page_cost = {"conservative": 1.15, "balanced": 1.1, "aggressive": 1.1}
        factor_default_statistics_target = {"conservative": 10, "balanced": 20, "aggressive": 50}
        factor_collapse_limits = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128} 

    factor_max_par_workers = {"conservative": 1, "balanced": 1.5, "aggressive": 2}
    factor_max_par_workers_gather = {"conservative": 1, "balanced": 1.5, "aggressive": 2}
    maintenance_work_mem_setting = {"conservative": 1, "balanced": 1, "aggressive": 2} 

    recommendations = {}
    for profile in ["conservative", "balanced", "aggressive"]:
        recommendations[profile] = {
            "shared_buffers": f"{int(base['shared_buffers'] * factor_shared_buffers[profile])}MB",
            #"work_mem": f"{int(base['work_mem'] * factor_general[profile])}kB",
            "work_mem": f"{int(work_mem_setting[profile] * 1024)}kB",
            #"maintenance_work_mem": f"{int(base['maintenance_work_mem'] * factor_general[profile])}MB",
            "maintenance_work_mem": f"{int(maintenance_work_mem_setting[profile] * 1024)}MB",
            #"effective_cache_size": f"{int(base['effective_cache_size'] * factor_general[profile])}MB", # Shash says the default 75% of mem never caused any issues, so leave it
            "random_page_cost": f"{base['random_page_cost'] * factor_random_page_cost[profile]}",
            "default_statistics_target": f"{int(base['default_statistics_target'] * factor_default_statistics_target[profile])}",
            "from_collapse_limit": f"{int(base['from_collapse_limit'] * factor_collapse_limits[profile])}",
            "join_collapse_limit": f"{int(base['join_collapse_limit'] * factor_collapse_limits[profile])}",
            "max_parallel_workers": f"{int(base['max_parallel_workers'] * factor_max_par_workers[profile])}",
            "max_worker_processes": f"{int(base['max_worker_processes'] * factor_max_par_workers[profile])}",
            "max_parallel_workers_per_gather": f"{int(base['max_parallel_workers_per_gather'] * factor_max_par_workers_gather[profile])}",
            "max_parallel_maintenance_workers": f"{int(base['max_parallel_maintenance_workers'] * factor_max_par_workers_gather[profile])}",
            "autovacuum": f"ON",
        }
    return recommendations


if inputs_enabled:
    recommendations = get_recommendations(int(memory_gb), db_role)
    table_data = {
        "Parameter": [],
        "Conservative Profile": [],
        "Balanced Profile": [],
        "Aggressive Profile": []
    }
    for param in recommendations["conservative"].keys():
        table_data["Parameter"].append(param)
        table_data["Conservative Profile"].append(recommendations["conservative"][param])
        table_data["Balanced Profile"].append(recommendations["balanced"][param])
        table_data["Aggressive Profile"].append(recommendations["aggressive"][param])
    df = pd.DataFrame(table_data)

    # --- Usage Auditing ---
    audit_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "support_ticket": support_ticket,
        "email": email,
        "db_role": db_role,
        "pg_version": pg_version,
        "server_cpus": server_cpus,
        "memory_gb": memory_gb,
        "recommendations": recommendations
    }

    audit_file = "usage_audit.jsonl"  # JSON Lines format
    audit_path = os.path.abspath(audit_file)
    try:
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry) + "\n")
        st.success(f"Audit entry saved to: {audit_path}")
    except Exception as e:
        st.error(f"Failed to write audit entry: {e}\nPath attempted: {audit_path}")

    st.dataframe(df, hide_index=True, height=500)
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, "postgresql_recommendations.csv", "text/csv")
else:
    st.info("Please enter both a valid Support Ticket ID and Email Address to use the advisor.")
