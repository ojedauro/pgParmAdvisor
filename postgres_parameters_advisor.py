import streamlit as st
import pandas as pd
import json
import re
import os
import requests
from datetime import datetime
import random

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

with st.sidebar:
    st.markdown('<img src="https://swimburger.net/media/ppnn3pcl/azure.png" style="height:40px; display:block; margin-left:auto; margin-right:auto;" alt="Azure" />', unsafe_allow_html=True)
    st.header("Input Configuration")

    email = st.text_input("Email Address")
    support_ticket = st.text_input("Support Ticket ID (optional)")

    def is_valid_email(email):
        pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
        return re.match(pattern, email)
    
    # inputs_enabled = bool(support_ticket.strip()) and bool(email.strip()) and is_valid_email(email)
    # if not bool(support_ticket.strip()):
    #     st.warning("Support Ticket ID is required.")
    
    inputs_enabled = bool(email.strip()) and is_valid_email(email)

    if not bool(email.strip()):
        st.warning("Email address is required.")
    elif email and not is_valid_email(email):
        st.warning("Please enter a valid email address.")

    with st.form(key="input_form"):
        db_role = st.selectbox("Database Role", ["OLTP", "OLAP", "RAG", "Mixed"], disabled=not inputs_enabled)
        pg_version = st.selectbox("PostgreSQL Version", ["17", "16", "15", "14", "13", "12"], disabled=not inputs_enabled)

        # Adjust CPU and memory options based on db_role
        cpu_options = [1,2,4,8,16,20,32,48,64,96,128,192]
        mem_options = [2,4,8,16,32,48,64,80,96,128,160,192,256,384,432,512,672,768,1024,1832]
        if db_role == "OLAP":
            # Remove CPUs 1-4 and memory 2,4 for OLAP
            cpu_options = [c for c in cpu_options if c > 4]
            mem_options = [m for m in mem_options if m > 4]
            st.info("For OLAP workloads, CPUs 1-4 and memory 2GB/4GB are not available.")

        server_cpus = st.selectbox("CPUs", cpu_options, disabled=not inputs_enabled)
        memory_gb = st.selectbox("Memory (GB)", mem_options, disabled=not inputs_enabled)
        submitted = st.form_submit_button("Submit")

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
        #"work_mem": int(memory * 1024 / max_connections), # Remove dependency of max_connections
        #"maintenance_work_mem": int(memory * 1024 * 0.1),
        "shared_buffers": int(memory * 1024 * 0.25),
        "effective_cache_size": int(memory * 1024 * 0.75),
        "random_page_cost": 1,
        "default_statistics_target": 100,
        "geqo_threshold": 12,
        "from_collapse_limit": 8,
        "join_collapse_limit": 8,
        "max_parallel_workers": (8 if server_cpus <= 16 else server_cpus / 2),
        "max_worker_processes": (8 if server_cpus <= 16 else server_cpus / 2),
        "max_parallel_workers_per_gather": (2 if server_cpus <= 8 else server_cpus / 4),
        "max_parallel_maintenance_workers": (2 if server_cpus <= 8 else server_cpus / 4),
    }

    if role == "OLTP":
        factor_random_page_cost = {"conservative": 1.2, "balanced": 1.1, "aggressive": 1.08}
        factor_default_statistics_target = {"conservative": 2, "balanced": 5, "aggressive": 10}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        work_mem_setting = {"conservative": 16, "balanced": 32, "aggressive": 64}
        geqo_threshold_setting = {"conservative": 16, "balanced": 20, "aggressive": 24} 
        factor_max_par_workers_gather = {"conservative": 2, "balanced": 4, "aggressive": 8}
    elif role == "OLAP":
        factor_random_page_cost = {"conservative": 1.1, "balanced": 1.08, "aggressive": 1.05} # Favours more full scans than using indexes
        factor_default_statistics_target = {"conservative": 4, "balanced": 8, "aggressive": 16}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.25, "aggressive": 1.6}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128}
        factor_max_par_workers_gather = {"conservative": 4, "balanced": 6, "aggressive": 8}
        if server_cpus <= 8:
            geqo_threshold_setting = {"conservative": 12, "balanced": 14, "aggressive": 16}
        else:
            geqo_threshold_setting = {"conservative": 16, "balanced": 24, "aggressive": 32}
    elif role == "RAG": # For RAG basically increase memory
        factor_random_page_cost = {"conservative": 1.15, "balanced": 1.1, "aggressive": 1.1}
        factor_default_statistics_target = {"conservative": 2, "balanced": 5, "aggressive": 10}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.25, "aggressive": 1.6}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128}
        geqo_threshold_setting = {"conservative": 16, "balanced": 22, "aggressive": 28}
        factor_max_par_workers_gather = {"conservative": 4, "balanced": 6, "aggressive": 8}
    else:  # Mixed
        factor_random_page_cost = {"conservative": 1.15, "balanced": 1.1, "aggressive": 1.1}
        factor_default_statistics_target = {"conservative": 2, "balanced": 5, "aggressive": 10}
        factor_shared_buffers = {"conservative": 1, "balanced": 1.2, "aggressive": 1.5}
        work_mem_setting = {"conservative": 32, "balanced": 64, "aggressive": 128}
        geqo_threshold_setting = {"conservative": 16, "balanced": 24, "aggressive": 32}
        factor_max_par_workers_gather = {"conservative": 4, "balanced": 6, "aggressive": 8}

    factor_max_par_workers = {"conservative": 1, "balanced": 1.5, "aggressive": 2}
    maintenance_work_mem_setting = {"conservative": 1, "balanced": 1, "aggressive": 2}

    recommendations = {}
    for profile in ["conservative", "balanced", "aggressive"]:
        recommendations[profile] = {
            #"effective_cache_size": f"{int(base['effective_cache_size'] * factor_general[profile])}MB", # Shash says the default 75% of mem never caused any issues, so leave it
            "shared_buffers": f"{int(base['shared_buffers'] * factor_shared_buffers[profile])}MB",
            "work_mem": f"{int(work_mem_setting[profile] * 1024)}kB",
            "maintenance_work_mem": f"{int(maintenance_work_mem_setting[profile] * 1024)}MB",
            "random_page_cost": f"{base['random_page_cost'] * factor_random_page_cost[profile]}",
            "default_statistics_target": f"{int(base['default_statistics_target'] * factor_default_statistics_target[profile])}",
            "geqo_threshold": f"{geqo_threshold_setting[profile]}",  # Assuming geqo_threshold is similar to default_statistics_target
            "from_collapse_limit": f"{int(geqo_threshold_setting[profile] * 0.75)}",
            "join_collapse_limit": f"{int(geqo_threshold_setting[profile] * 0.75)}",
            "max_parallel_workers": f"{int((8 if server_cpus <= 16 else base['max_parallel_workers'] * factor_max_par_workers[profile]))}",
            "max_worker_processes": f"{int(8 if server_cpus <= 8 else server_cpus)}",
            "max_parallel_workers_per_gather": f"{int(2 if server_cpus <= 8 else factor_max_par_workers_gather[profile])}",
            "max_parallel_maintenance_workers": f"{int(2 if server_cpus <= 8 else factor_max_par_workers_gather[profile])}",
            "autovacuum": f"ON",
        }
    return recommendations

# Only process and save when the form is submitted
if 'submitted' in locals() and submitted and inputs_enabled:
    # If OLAP and CPUs 1-4, show warning instead of table
    if db_role == "OLAP" and server_cpus in [1,2,4]:
        st.warning("For OLAP workloads, CPUs 1-4 are not supported. Please select a higher CPU value.")
    else:
        recommendations = get_recommendations(int(memory_gb), db_role)
        table_data = {
            "Parameter": [],
            "Conservative Profile": [],
            "Balanced Profile": [],
            "Aggressive Profile": [],
            "Apply type": []
        }
        for param in recommendations["conservative"].keys():
            table_data["Parameter"].append(param)
            table_data["Conservative Profile"].append(recommendations["conservative"][param])
            table_data["Balanced Profile"].append(recommendations["balanced"][param])
            table_data["Aggressive Profile"].append(recommendations["aggressive"][param])
            table_data["Apply type"].append("Static" if param in ["shared_buffers", "max_worker_processes"] else "Dynamic")
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

        # Azure Blob SAS URL
        AZURE_BLOB_SAS_URL = st.secrets["AZURE_URL"] + st.secrets["AUDIT_FILE"] + "?" + st.secrets["AZURE_TOKEN"] # Get secrets from Streamlit secrets management

        # Try to append the entry to the blob
        try:
            # Download current blob content
            response = requests.get(AZURE_BLOB_SAS_URL)
            if response.status_code == 200:
                current_content = response.text
                if current_content and not current_content.endswith('\n'):
                    current_content += '\n'
            elif response.status_code == 404:
                current_content = ''
            else:
                raise Exception(f"Failed to read blob: {response.status_code} {response.text}")

            # Append new entry
            new_content = current_content + json.dumps(audit_entry) + "\n"
            put_response = requests.put(AZURE_BLOB_SAS_URL, data=new_content.encode('utf-8'), headers={"x-ms-blob-type": "BlockBlob"})
            if not put_response.status_code in [201, 200]:
                st.error(f"Failed to write audit entry to blob: {put_response.status_code} {put_response.text}")

        except Exception as e:
            st.error(f"Failed to write audit entry to Azure Blob: {e}")

        st.dataframe(df, hide_index=True, height=500)
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "postgresql_recommendations.csv", "text/csv")
elif 'submitted' in locals() and submitted and not inputs_enabled:
    st.info("Please enter both a valid Support Ticket ID and Email Address to use the advisor.")
