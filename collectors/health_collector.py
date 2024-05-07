from prometheus_client.core import GaugeMetricFamily

import logging
import math
import re
import datetime
import requests

class HealthCollector(object):

    def __enter__(self):
        return self

    def __init__(self, redfish_metrics_collector):

        self.col = redfish_metrics_collector

        self.health_metrics = GaugeMetricFamily(
            "redfish_health",
            "Redfish Server Monitoring Health Data",
            labels=self.col.labels,
        )
        self.mem_metrics_correctable = GaugeMetricFamily(
            "redfish_memory_correctable",
            "Redfish Server Monitoring Memory Data for correctable errors",
            labels=self.col.labels,
        )
        self.mem_metrics_unorrectable = GaugeMetricFamily(
            "redfish_memory_uncorrectable",
            "Redfish Server Monitoring Memory Data for uncorrectable errors",
            labels=self.col.labels,
        )
    
    def parse_nvme_info(self, providing_drives):
        attributes = {
             "media_errors": "",
             "percentage_used": "",
             "power_cycles_count": "",
             "power_on_hours": "",
             "smartctl_error": "",
             "temperature": ""
             }
        
        oem_data = providing_drives.get("Oem", {})
        smart_data = oem_data.get("SmartData", {})
        new_labels = {"disk": "","type": providing_drives.get("MediaType", "").lower()}
        new_labels.update(self.col.labels)
        if not smart_data or not oem_data:
           self.health_metrics.add_sample("smartmon_device_active", value=0, labels=new_labels)
           self.health_metrics.add_sample("smartmon_device_smart_available", value=0, labels=new_labels)
           self.health_metrics.add_sample("smartmon_device_smart_enabled", value=0, labels=new_labels)
           return

        disk_name = None
        for key in smart_data.keys():
           if "[" in key and "]" in key:
               disk_name = key.split("[")[1].split("]")[0]
               break

        for key, value in smart_data.items():
            if 'hours' in key:
                attributes["power_on_hours"] = value.lower()
            elif 'temperature' in key or 'Temperature' in key:
                attributes["temperature"] = value.lower()
            elif 'Cycle' in key or 'cycles' in key:
                attributes["power_cycles_count"] = value.lower()
            elif 'percentage' in key or 'Percentage' in key:
                attributes["percentage_used"] = value.lower()
            elif 'media' in key or 'Media' in key:
                attributes["media_errors"] = value.lower()

        current_labels = {"disk": f"/dev/{disk_name}", "type": providing_drives.get("MediaType", "").lower()}
        current_labels.update(self.col.labels)
        val=1
        self.health_metrics.add_sample("smartmon_device_active", value=val, labels=current_labels)
        self.health_metrics.add_sample("smartmon_device_smart_available", value=val, labels=current_labels)
        self.health_metrics.add_sample("smartmon_device_smart_enabled", value=val, labels=current_labels)

        # smartmon_device_smart_healthy
        smart_health = math.nan
        smart_status = dict( (k.lower(), v) for k, v in providing_drives["Status"].items() )
        if "state" in smart_status and smart_status["state"] != "absent":
            smart_health = ( math.nan if smart_status["state"]  is None else self.col.status[smart_status["state"].lower()] )
            if smart_health is math.nan:
                logging.warning(f"Target {self.col.target}: Host {self.col.host}, Model {device_model}: No health data found.")
        self.health_metrics.add_sample("smartmon_device_smart_healthy", value=smart_health, labels=current_labels)

        # smartmon_device_info
        info_labels = {"disk": f"/dev/{disk_name}","type": providing_drives.get("MediaType", "").lower(),"serial_number": providing_drives.get("Id", ""),"model_family": providing_drives.get("Model", "").lower(),"host":self.col.host }
        self.health_metrics.add_sample("smartmon_device_info", value=smart_health, labels=info_labels)
        

        # smartmon_temperature_celsius_raw_value
        temperature_value = ''.join(filter(str.isdigit, attributes.get("temperature", "")))
        if temperature_value and temperature_value.isdigit():
            temperature_float = float(temperature_value)
            self.health_metrics.add_sample("smartmon_temperature_celsius_raw_value", value=temperature_float, labels=current_labels)
        else:
            pass

        # smartmon_power_cycles_count_raw_value
        power_cycle = attributes.get("power_cycles_count", "")
        if power_cycle and power_cycle.isdigit():
            power_cycle = int(power_cycle)
            self.health_metrics.add_sample("smartmon_power_cycle_count_raw_value", value=power_cycle, labels=current_labels)
        else:
            pass

        # smartmon_power_on_hours_raw_value
        power_on_hours = attributes.get("power_on_hours", "")
        if power_on_hours and power_on_hours.isdigit():
            power_on_hours = int(power_on_hours)
            self.health_metrics.add_sample("smartmon_power_on_hours_raw_value", value=power_on_hours, labels=current_labels)
        else:
            pass

        # smartmon_percentage_used_raw_value
        percentage_used = attributes.get("percentage_used", "")
        if  percentage_used and percentage_used[:-1].isdigit():
            percentage_used = int(percentage_used[:-1])
            self.health_metrics.add_sample("smartmon_percentage_used_raw_value", value=percentage_used, labels=current_labels)
        else:
            pass

        # smartmon_media_and_data_integrity_errors_count_raw_value
        media_errors = attributes.get("media_errors", "")
        if media_errors and media_errors.isdigit():
            media_errors = int(media_errors)
            self.health_metrics.add_sample("smartmon_media_and_data_integrity_errors_count_raw_value", value=media_errors, labels=current_labels)
        else:
            pass

        # smartmon_smartctl_run
        run_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        self.health_metrics.add_sample("smartmon_smartctl_run", value=run_time, labels=current_labels)
        

    def parse_scsi_info(self, providing_drives):
        attributes = {
             "exit_status": "",
             "grown_defects_count": "",
             "percentage_used": "",
             "power_on_hours": "",
             "power_cycles_count": "",
             "smartctl_error": "",
             "temperature": ""
             }
    
        oem_data = providing_drives.get("Oem", {})
        smart_data = oem_data.get("SmartData", {})
        new_labels = {"disk": "","type": providing_drives.get("MediaType", "").lower()}
        new_labels.update(self.col.labels)
        if not smart_data or not oem_data:
           self.health_metrics.add_sample("smartmon_device_active", value=0, labels=new_labels)
           self.health_metrics.add_sample("smartmon_device_smart_available", value=0, labels=new_labels)
           self.health_metrics.add_sample("smartmon_device_smart_enabled", value=0, labels=new_labels)
           return

        disk_name = None
        for key in smart_data.keys():
           if "[" in key and "]" in key:
               disk_name = key.split("[")[1].split("]")[0]
               break
        
        for key, value in smart_data.items():
            if 'hours' in key:
                attributes["power_on_hours"] = value.lower()
            elif 'temperature' in key or 'Temperature' in key:
                attributes["temperature"] = value.lower()
            elif 'Cycle' in key or 'cycles' in key:
                attributes["power_cycles_count"] = value.lower()
            elif 'percentage' in key or 'Percentage' in key:
                attributes["percentage_used"] = value.lower()
    
        current_labels = {"disk": f"/dev/{disk_name}", "type": providing_drives.get("MediaType", "").lower()}
        current_labels.update(self.col.labels)
        val=1
        self.health_metrics.add_sample("smartmon_device_active", value=val, labels=current_labels)
        self.health_metrics.add_sample("smartmon_device_smart_available", value=val, labels=current_labels)
        self.health_metrics.add_sample("smartmon_device_smart_enabled", value=val, labels=current_labels)

        # smartmon_device_smart_healthy
        smart_health = math.nan
        smart_status = dict( (k.lower(), v) for k, v in providing_drives["Status"].items() )
        if "state" in smart_status and smart_status["state"] != "absent":
            smart_health = ( math.nan if smart_status["state"]  is None else self.col.status[smart_status["state"].lower()] )
            if smart_health is math.nan:
                logging.warning(f"Target {self.col.target}: Host {self.col.host}, Model {device_model}: No health data found.")
        self.health_metrics.add_sample("smartmon_device_smart_healthy", value=smart_health, labels=current_labels)

        # smartmon_device_info
        info_labels = {"disk": f"/dev/{disk_name}", "type": providing_drives.get("MediaType", "").lower(),"serial_number": providing_drives.get("Id", ""),"model_family": providing_drives.get("Model", "").lower() }
        self.health_metrics.add_sample("smartmon_device_info", value=smart_health, labels=info_labels)

        # smartmon_temperature_celsius_raw_value
        temperature_value = ''.join(filter(str.isdigit, attributes.get("temperature", "")))
        if temperature_value and temperature_value.isdigit():
            temperature_float = float(temperature_value)
            self.health_metrics.add_sample("smartmon_temperature_celsius_raw_value", value=temperature_float, labels=current_labels)
        else:
            pass

        # smartmon_power_cycles_count_raw_value
        power_cycle = attributes.get("power_cycles_count", "")
        if  power_cycle and power_cycle.isdigit():
            power_cycle = int(power_cycle)
            self.health_metrics.add_sample("smartmon_power_cycle_count_raw_value", value=power_cycle, labels=current_labels)
        else:
            pass

        # smartmon_power_on_hours_raw_value
        power_on_hours = attributes.get("power_on_hours", "")
        if  power_on_hours and power_on_hours.isdigit():
            power_on_hours = int(power_on_hours)
            self.health_metrics.add_sample("smartmon_power_on_hours_raw_value", value=power_on_hours, labels=current_labels)
        else:
            pass

        # smartmon_percentage_used_raw_value
        percentage_used = attributes.get("percentage_used", "")
        if  percentage_used and percentage_used.isdigit():
            percentage_used = int(percentage_used.strip('%'))
            self.health_metrics.add_sample("smartmon_percentage_used_raw_value", value=percentage_used, labels=current_labels)
        else:
            pass
        
        # smartmon_smartctl_run
        run_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        self.health_metrics.add_sample("smartmon_smartctl_run", value=run_time, labels=current_labels)

    def get_smart_data(self):        

        logging.debug(f"Target {self.col.target}: Get the SMART data.")
        storage_services_collection = self.col.connect_server(self.col.urls["StorageServices"])
        if not storage_services_collection or 'Members' not in storage_services_collection:
           return

        for storage_service_member in storage_services_collection["Members"]:
            for key, storage_url in storage_service_member.items():
               if key.startswith("enclosure_id") and storage_url.startswith("/redfish/v1/") and not storage_url.endswith("NULL"):
                  storage_service = self.col.connect_server(storage_url)
                  type_storage_service = type(storage_service)
                  
                  if type_storage_service is int and storage_service not in (200, 201):
                      logging.info("No response from Storage service endpoint")
                      continue
                  elif storage_service is not None and 'StoragePools' in storage_service:
                      storage_pool_collection = self.col.connect_server(storage_service["StoragePools"]['@odata.id'])
                  else:
                      logging.info("StoragePools endpoint does not exist")
                      continue
                  
                  if not storage_pool_collection or 'Members' not in storage_pool_collection:
                     continue

                  for storage_pool_member in storage_pool_collection["Members"]:
                      for pool, pool_url in storage_pool_member.items():
                          if pool.startswith("enclosure_id") and pool_url.startswith("/redfish/v1/") and not pool_url.endswith("NULL"):
                             storage_pool = self.col.connect_server(pool_url)
                             type_storage_pool = type(storage_pool)
                  
                             if type_storage_pool is int and storage_pool not in (200, 201):
                                 logging.info("No response from Storage pool endpoint")
                                 continue
                             elif storage_pool is not None and 'CapacitySources' in storage_pool:
                                 self.col.urls["CapacitySources"]=f"{storage_pool['@odata.id']}/CapacitySources"
                                 capacity_source_collection = self.col.connect_server(self.col.urls["CapacitySources"])
                             else:
                                 logging.info("CapacitySources endpoint does not exist")
                                 continue
                             
                             if not capacity_source_collection or 'Members' not in capacity_source_collection:
                                continue

                             for capacity_source_member in capacity_source_collection["Members"]:
                                 for capacity, capacity_url in capacity_source_member.items():
                                     if capacity.startswith("@odata.id") and capacity_url.startswith("/redfish/v1/") and not capacity_url.endswith("NULL"):
                                        capacity_source = self.col.connect_server(capacity_url)
                                        type_capacity_source = type(capacity_source)
                                        
                                        if type_capacity_source is int and capacity_source not in (200, 201):
                                            logging.info("No response from Capacity source endpoint")
                                            continue
                                        elif capacity_source is not None and 'CapacitySources' in storage_pool:
                                             self.col.urls["ProvidingDrives"]=f"{capacity_source['@odata.id']}/ProvidingDrives"
                                             providing_drives_collection = self.col.connect_server(self.col.urls["ProvidingDrives"])
                                        else:
                                            logging.info("ProvidingDrives endpoint does not exist")
                                            continue

                                        if not providing_drives_collection or 'Members' not in providing_drives_collection:
                                           continue

                                        for providing_drives_member in providing_drives_collection["Members"]:
                                            for drives, drives_url in providing_drives_member.items():
                                                if drives.startswith("Bay") and drives_url.startswith("/redfish/v1") and not drives_url.endswith("NULL"):
                                                   providing_drives = self.col.connect_server(drives_url)
                                                   type_providing_drives = type(providing_drives)

                                                   if type_providing_drives is int and providing_drives not in (200, 201):
                                                       logging.info("No response from Providing Drives endpoint")
                                                       continue
                                                   elif providing_drives is not None and "@odata.id" in providing_drives:
                                                       media_type = providing_drives["MediaType"].lower()
                                                       
                                                       if media_type == "nvme":
                                                           self.parse_nvme_info(providing_drives)
                                                       elif media_type == "sas":
                                                           self.parse_scsi_info(providing_drives)
                                                       else:
                                                           continue
                                                   else:
                                                       continue

    def collect(self):

        logging.info(f"Target {self.col.target}: Collecting data ...")
        current_labels = {"device_type": "system", "device_name": "summary"}
        current_labels.update(self.col.labels)
        self.health_metrics.add_sample(
            "redfish_health", value=self.col.server_health, labels=current_labels
        )
   
        # Export the SMART data
        if self.col.urls["StorageServices"]:
            self.get_smart_data()
        else:
            logging.warning(f"Target {self.col.target}: No SMART data provided! Cannot get SMART data!")
           
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            logging.exception(f"Target {self.target}: An exception occured in {exc_tb.tb_frame.f_code.co_filename}:{exc_tb.tb_lineno}")
