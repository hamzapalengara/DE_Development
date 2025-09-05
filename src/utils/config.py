"""
Configuration management for the data pipeline
Simple implementation for learning purposes
"""

import yaml
import os
from typing import Dict, Any

class Config:
    """Simple configuration loader"""
    
    def __init__(self, config_path: str = None):
        """
        Initialize config loader
        
        Args:
            config_path: Path to config file, defaults to config/pipeline_config.yaml
        """
        if config_path is None:
            # Get project root directory
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(project_root, "config", "pipeline_config.yaml")
        
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Simple variable replacement for ${paths.base_path}
        base_path = config['paths']['base_path']
        
        # Replace variables in paths
        for category in config['paths']:
            if isinstance(config['paths'][category], dict):
                for key, value in config['paths'][category].items():
                    if isinstance(value, str) and '${paths.base_path}' in value:
                        config['paths'][category][key] = value.replace('${paths.base_path}', base_path)
        
        return config
    
    def get_path(self, path_key: str) -> str:
        """
        Get a path from config
        
        Args:
            path_key: Key like 'bronze.products' or 'landing.batch'
        
        Returns:
            The resolved path
        """
        category, key = path_key.split('.')
        return self.config['paths'][category][key]
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configurations"""
        return self.config['spark']['configs']
    
    def get_app_name(self) -> str:
        """Get Spark application name"""
        return self.config['spark']['app_name']
    
    def get_batch_size(self) -> int:
        """Get batch processing size"""
        return self.config['processing']['batch_size']
    
    def get_error_threshold(self) -> float:
        """Get error threshold"""
        return self.config['processing']['error_threshold']