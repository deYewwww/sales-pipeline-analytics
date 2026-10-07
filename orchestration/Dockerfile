FROM astrocrpublic.azurecr.io/runtime:3.3-8

# dbt Fusion: standalone binary, no Python packages, so no conflicts with Airflow's libraries
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --version 2.0.6
ENV PATH="/home/astro/.local/bin:${PATH}"
