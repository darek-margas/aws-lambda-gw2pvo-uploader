import json
import logging
from datetime import datetime
from gw2pvo import gw_api, pvo_api, ds_api, netatmo_api

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    try:
        # Extract configuration from environment variables or event
        config = get_config(event)

        # Run the main logic
        result = run_once(config)

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_config(event):
    # Extract configuration from environment variables or event
    # You'll need to set these in the Lambda function configuration
    return {
        'gw_station_id': event.get('gw_station_id'),
        'gw_account': event.get('gw_account'),
        'gw_password': event.get('gw_password'),
        'pvo_system_id': event.get('pvo_system_id'),
        'pvo_api_key': event.get('pvo_api_key'),
        'darksky_api_key': event.get('darksky_api_key'),
        'pv_voltage': event.get('pv_voltage', False),
        'skip_offline': event.get('skip_offline', False),
        'netatmo_username': event.get('netatmo_username'),
        'netatmo_password': event.get('netatmo_password'),
        'netatmo_client_id': event.get('netatmo_client_id'),
        'netatmo_client_secret': event.get('netatmo_client_secret'),
        'netatmo-device-id': event.get('netatmo-device-id'),
    }

def run_once(config):
    # Fetch the last reading from GoodWe
    gw = gw_api.GoodWeApi(config['gw_station_id'], config['gw_account'], config['gw_password'])
    data = gw.getCurrentReadings()

    # Check if we want to abort when offline
    if config['skip_offline'] and data['status'] == 'Offline':
        logger.info("Skipped upload as the inverter is offline")
        return {"message": "Skipped upload (offline)"}

    # Get temperature if Dark Sky API key is provided
    if config['darksky_api_key']:
        ds = ds_api.DarkSkyApi(config['darksky_api_key'])
        temperature = ds.get_temperature(data['latitude'], data['longitude'])
        if temperature:
            logger.info(f"Current local temperature is {temperature:.1f} °C")
            data['temperature'] = temperature

    voltage = data['grid_voltage']
    if config['pv_voltage']:
        voltage = data['pv_voltage']

    # Submit reading to PVOutput
    if config['pvo_system_id'] and config['pvo_api_key']:
        pvo = pvo_api.PVOutputApi(config['pvo_system_id'], config['pvo_api_key'])
        pvo.add_status(data['pgrid_w'], data['eday_kwh'], data.get('temperature'), voltage, data['load'], data['consumptionOfLoad'],data['itemp'])
        return {"message": "Data uploaded successfully", "data": data}
    else:
        logger.warning("Missing PVO id and/or key")
        return {"message": "Missing PVO id and/or key", "data": data}

