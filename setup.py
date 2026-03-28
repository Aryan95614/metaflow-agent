from setuptools import setup, find_packages

setup(
    name="metaflow-agent",
    version="0.2.0",
    description="Agent-friendly Metaflow client with paginated metadata access",
    packages=find_packages(include=[
        "metaflow_agent", "metaflow_agent.*",
        "metaflow_extensions.*",
        "agent_utils",
    ]),
    install_requires=["metaflow>=2.12"],
    python_requires=">=3.8",
)
