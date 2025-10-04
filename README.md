# CRYPTO CURRENCY ANALYSIS PIPELINE 🚀

## 📋 Descripción del Proyecto
Pipeline ETL automatizado para análisis de tendencias en criptomonedas, que combina datos históricos de Kaggle con información en tiempo real de CoinGecko API para clasificar tendencias de mercado.


## 🏗️ Arquitectura del Pipeline

### Flujo de Procesamiento
[Kaggle CSV] → [Limpieza] → [Dataset Histórico Limpio]
↓
[CoinGecko API] → [Clasificación] → [Dataset Tendencias] → [Reportes]

### Tasks del DAG
1. **ingest_api_data** - Extracción datos API CoinGecko
2. **clean_historical_data** - Limpieza dataset histórico Kaggle  
3. **classify_api_data** - Clasificación de tendencias
4. **generate_data_quality_reports** - Generación de reportes de calidad
5. **download_final_data** - Descarga datasets procesados


## 📁 Estructura de Archivos

🔹 **airflow/**
   📂 **dags/**
      📄 `crypto_data_pipeline_final_project.py` - Pipeline principal
   
   📂 **include/**
      📊 `CryptocurrencyData.csv` - Dataset histórico original
      📝 `data_dictionary.txt` - Diccionario de datos
      🧹 `crypto_historical_cleaned_*.csv` - Histórico procesado
      🎯 `crypto_api_classified_*.csv` - Datos clasificados
   
   📂 **logs/**
      📋 Logs de ejecución


## 🚀 Ejecución
### Automática
- El DAG se activa automáticamente según el schedule programado. Se ejecuta cada 6 horas automáticamente

### Manual
1. Airflow UI → DAGs
2. Seleccionar `crypto_data_pipeline_final_project`
3. Click "Trigger DAG"
4. Monitorear en Graph View


## 📊 Outputs
### Archivos Generados
- `crypto_historical_cleaned_TIMESTAMP.csv`
- `crypto_api_classified_TIMESTAMP.csv`

### Clasificación de Tendencias
- **FUERTE ALCISTA**: Cambio ≥ 5%
- **MODERADA ALCISTA**: Cambio 1% - 5%
- **ESTABLE**: Cambio -1% - 1%
- **MODERADA BAJISTA**: Cambio -5% - -1%
- **FUERTE BAJISTA**: Cambio ≤ -5%


## 🔧 Troubleshooting
### Errores Comunes
- **API Rate Limits**: Reintentos automáticos configurados
- **Archivos no encontrados**: Verificar ruta en `include/`
- **Procesamiento lento**: Optimizar tamaño de datasets

### Monitoreo
- Revisar logs en Airflow UI por task
- Verificar archivos en `include/`
- Monitorear uso de memoria


## 📈 Métricas de Calidad
- Completitud: > 95% de datos procesados exitosamente
- Actualidad: Datos actualizados cada 6 horas
- Precisión: Clasificación con > 85% de accuracy
- Disponibilidad: 99.5% uptime del pipeline


## 🔄 Mantenimiento
### Actualizaciones
- Revisar diccionario de datos en include/data_dictionary.txt
- Actualizar dependencias periódicamente
- Verificar cambios en API CoinGecko

### Backup
- Los archivos source se mantienen en include/
- Los outputs incluyen timestamp para versionado
- Logs detallados en Airflow UI


## Próximas Mejoras:
🔮 Integración con machine learning
🔮 Dashboard en tiempo real
🔮 Alertas automáticas por email


## 📞 Soporte
- Diccionario de datos: `include/data_dictionary.txt`
- Código fuente: `dags/crypto_data_pipeline_final_project.py`
- Logs: Airflow UI → Browse Logs


## ☎️ Contacto
- Ronald Chipana Wariste
- Luz Alizon Mamani Mena
- Roni Edwin Oyardo Acuña
- Ever Alcides Soto Palli
