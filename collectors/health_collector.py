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
             "available_spare": "",
             "available_spare_threshold": "",
             "controller_busy_time": "",
             "error_information_log_entries": "",
             "exit_status": "",
             "firmware_version": "",
             "host_read": "",
             "host_write": "",
             "media_errors": "",
             "percentage_used": "",
             "power_cycles_count": "",
             "power_on_hours": "",
             "smartctl_error": "",
             "temperature": "",
             "unsafe_shutdowns": ""
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
            if 'spare' in key and not 'threshold' in key:
                attributes["available_spare"] = value.lower()
            elif 'spare' in key and 'threshold' in key:
                attributes["available_spare_threshold"] = value.lower()
            elif 'controller' in key:
                attributes["controller_busy_time"] = value.lower()
            elif 'information' in key:
                attributes["error_information_log_entries"] = value.lower()
            elif 'firmware' in key:
                attributes["firmware_version"] = value.lower()
            elif 'write' in key:
                attributes["host_write"] = value.lower()
            elif 'read' in key:
                attributes["host_read"] = value.lower()
            elif 'hours' in key:
                attributes["power_on_hours"] = value.lower()
            elif 'shutdowns' in key:
                attributes["unsafe_shutdowns"] = value.lower()
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
                logging.warning(f"Target {self.col.target}: Host {self.col.host}, Model {providing_drives.get('Model', 'Unknown')}: No health data found.")
        self.health_metrics.add_sample("smartmon_device_smart_healthy", value=smart_health, labels=current_labels)

        # smartmon_device_info
        info_labels = {"redfish_instance": f"{self.col.target}:9220","disk": f"/dev/{disk_name}","type": providing_drives.get("MediaType", "").lower(),"serial_number": providing_drives.get("Id", ""),"model_family": providing_drives.get("Model", "").lower(),"host":self.col.host }
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

        # smartmon_unsafe_shutdowns_count_raw_value
        unsafe_shutdowns = attributes.get("unsafe_shutdowns", "")
        if unsafe_shutdowns and unsafe_shutdowns.isdigit():
            unsafe_shutdowns = int(unsafe_shutdowns)
            self.health_metrics.add_sample("smartmon_unsafe_shutdowns_count_raw_value", value=unsafe_shutdowns, labels=current_labels)
        else:
            pass

        # smartmon_error_information_log_entries_raw_value
        error_info = attributes.get("error_information_log_entries", "")
        if error_info and error_info.isdigit():
            error_info = int(error_info)
            self.health_metrics.add_sample("smartmon_error_information_log_entries_raw_value", value=error_info, labels=current_labels)
        else:
            pass

        # smartmon_host_read_commands_raw_value
        host_read = attributes.get("host_read", "")
        if host_read and host_read.isdigit():
            host_read = int(host_read)
            self.health_metrics.add_sample("smartmon_host_read_commands_raw_value", value=host_read, labels=current_labels)
        else:
            pass

        # smartmon_host_write_commands_raw_value
        host_write = attributes.get("host_write", "")
        if host_write and host_write.isdigit():
            host_write = int(host_write)
            self.health_metrics.add_sample("smartmon_host_write_commands_raw_value", value=host_write, labels=current_labels)
        else:
            pass

        # smartmon_controller_busy_time_raw_value
        cont_busy = attributes.get("controller_busy_time", "")
        if cont_busy and cont_busy.isdigit():
            cont_busy = int(cont_busy)
            self.health_metrics.add_sample("smartmon_controller_busy_time_raw_value", value=cont_busy, labels=current_labels)
        else:
            pass

        # smartmon_available_spare_raw_value
        available_spare = attributes.get("available_spare", "")
        if available_spare and available_spare.isdigit():
            available_spare = int(available_spare)
            self.health_metrics.add_sample("smartmon_available_spare_raw_value", value=available_spare, labels=current_labels)
        else:
            pass

        # smartmon_available_spare_threshold
        spare_threshold = attributes.get("available_spare_threshold", "")
        if spare_threshold and spare_threshold.isdigit():
            spare_threshold = int(spare_threshold)
            self.health_metrics.add_sample("smartmon_available_spare_threshold_raw_value", value=spare_threshold, labels=current_labels)
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
            if 'defects' in key:
                attributes["grown_defects_count"] = value.lower()
            elif 'hours' in key:
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
                logging.warning(f"Target {self.col.target}: Host {self.col.host}, Model {providing_drives.get('Model', 'Unknown')}: No health data found.")
        self.health_metrics.add_sample("smartmon_device_smart_healthy", value=smart_health, labels=current_labels)

        # smartmon_device_info
        info_labels = {"redfish_instance": f"{self.col.target}:9220", "disk": f"/dev/{disk_name}", "type": providing_drives.get("MediaType", "").lower(),"serial_number": providing_drives.get("Id", ""),"model_family": providing_drives.get("Model", "").lower() }
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
        
        # smartmon_grown_defects_count_raw_value
        grown_defect = attributes.get("grown_defects_count", "")
        if grown_defect and grown_defect.isdigit():
            grown_defect = int(grown_defect)
            self.health_metrics.add_sample("smartmon_grown_defects_count_raw_value", value=grown_defect, labels=current_labels)
        else:
            pass

        # smartmon_smartctl_run
        run_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        self.health_metrics.add_sample("smartmon_smartctl_run", value=run_time, labels=current_labels)


    def get_smart_data(self):        

        logging.debug(f"Target {self.col.target}: Get the SMART data.")
        storage_services_collection = self.col.connect_server(self.col.urls["StorageServices"])
        logging.debug(f"Target {self.col.target}: Retrieved storage services collection")
        if not storage_services_collection or 'Members' not in storage_services_collection:
           logging.debug(f"Target {self.col.target}: No storage services members found")
           return

        for storage_service_member in storage_services_collection["Members"]:
            for key, storage_url in storage_service_member.items():
               logging.debug(f"Target {self.col.target}: Processing storage service: {storage_url}")
               if storage_url.split("/")[-1].startswith("lustre-") or not storage_url.startswith("/redfish/v1/"):
                   logging.debug(f"Target {self.col.target}: Skipping storage service: {storage_url}")
                   continue
               storage_service = self.col.connect_server(storage_url)
               if isinstance(storage_service, int) and storage_service not in (200, 201):
                   logging.info("No response from Storage service endpoint")
                   continue
               elif storage_service is not None and 'StoragePools' in storage_service:
                   storage_pool_collection = self.col.connect_server(storage_service["StoragePools"]['@odata.id'])
                   logging.debug(f"Target {self.col.target}: Found storage pools endpoint")
               else:
                   logging.info("StoragePools endpoint does not exist")
                   continue
                  
               if not storage_pool_collection or 'Members' not in storage_pool_collection:
                   continue

               for storage_pool_member in storage_pool_collection["Members"]:
                   for pool, pool_url in storage_pool_member.items():
                       logging.debug(f"Target {self.col.target}: Processing storage pool: {pool_url}")
                       if not pool_url.startswith("/redfish/v1/") or pool_url.endswith("NULL"):
                          logging.debug(f"Target {self.col.target}: Skipping invalid pool URL: {pool_url}")
                          continue
                       storage_pool = self.col.connect_server(pool_url)
                       if isinstance(storage_pool, int) and storage_pool not in (200, 201):
                          logging.debug("Target %s: No response from Storage pool endpoint: %s", self.col.target, pool_url)
                          continue
                       elif storage_pool is not None and 'CapacitySources' in storage_pool:
                          self.col.urls["CapacitySources"]=f"{storage_pool['@odata.id']}/CapacitySources"
                          logging.debug(f"Target {self.col.target}: Found CapacitySources endpoint: {self.col.urls['CapacitySources']}")
                          capacity_source_collection = self.col.connect_server(self.col.urls["CapacitySources"])
                       else:
                          logging.debug("Target %s: CapacitySources endpoint does not exist for pool %s", self.col.target, pool_url)
                          continue
                             
                       if not capacity_source_collection or 'Members' not in capacity_source_collection:
                          continue

                       for capacity_source_member in capacity_source_collection["Members"]:
                          for capacity, capacity_url in capacity_source_member.items():
                              logging.debug(f"Target {self.col.target}: Processing capacity source: {capacity_url}")
                              if not capacity_url.startswith("/redfish/v1/") or capacity_url.endswith("NULL"):
                                 logging.debug(f"Target {self.col.target}: Skipping invalid capacity URL: {capacity_url}")
                                 continue
                              capacity_source = self.col.connect_server(capacity_url)
                              if isinstance(capacity_source, int) and capacity_source not in (200, 201):
                                 logging.debug("Target %s: No response from Capacity source endpoint: %s", self.col.target, capacity_url)
                                 continue
                              elif capacity_source is not None and 'ProvidingDrives' in capacity_source:
                                 self.col.urls["ProvidingDrives"]=f"{capacity_source['@odata.id']}/ProvidingDrives"
                                 logging.debug(f"Target {self.col.target}: Found ProvidingDrives endpoint: {self.col.urls['ProvidingDrives']}")
                                 providing_drives_collection = self.col.connect_server(self.col.urls["ProvidingDrives"])
                              else:
                                 logging.debug("Target %s: ProvidingDrives endpoint does not exist for capacity %s", self.col.target, capacity_url)
                                 continue

                              if not providing_drives_collection or 'Members' not in providing_drives_collection:
                                 continue

                              for providing_drives_member in providing_drives_collection["Members"]:
                                  for drives, drives_url in providing_drives_member.items():
                                      logging.debug(f"Target {self.col.target}: Processing drive: {drives_url}")
                                      if not drives_url.startswith("/redfish/v1") or drives_url.endswith("NULL"):
                                          logging.debug(f"Target {self.col.target}: Skipping invalid drive URL: {drives_url}")
                                          continue
                                      providing_drives = self.col.connect_server(drives_url)
                                      if isinstance(providing_drives, int) and providing_drives not in (200, 201):
                                          logging.debug("Target %s: No response from Providing Drives endpoint: %s", self.col.target, drives_url)
                                          continue
                                      elif providing_drives is not None and "@odata.id" in providing_drives:
                                          media_type = providing_drives["MediaType"].lower()
                                          logging.debug(f"Target {self.col.target}: Processing drive with media type: {media_type}")
                                                       
                                          if media_type == "nvme":
                                              self.parse_nvme_info(providing_drives)
                                          elif media_type == "sas":
                                              self.parse_scsi_info(providing_drives)
                                          else:
                                              logging.debug(f"Target {self.col.target}: Unsupported media type: {media_type}")
                                              continue
                                      else:
                                          logging.debug(f"Target {self.col.target}: Invalid drive data received from: {drives_url}")
                                          continue

    def collect(self):

        logging.info("Target %s: Collecting health data ...", self.col.target)
        current_labels = {"device_type": "system", "device_name": "summary"}
        current_labels.update(self.col.labels)
        self.health_metrics.add_sample(
            "redfish_health", value=self.col.server_health, labels=current_labels
        )
   
        # Export the SMART data
        if self.col.urls["StorageServices"]:
            logging.debug("Target %s: Starting SMART data collection", self.col.target)
            self.get_smart_data()
            logging.debug("Target %s: Completed SMART data collection", self.col.target)
        else:
            logging.warning("Target %s: No SMART data provided! Cannot get SMART data!", self.col.target)
           
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            logging.exception(
                "Target %s: An exception occured in %s:%s",
                self.col.target,
                exc_tb.tb_frame.f_code.co_filename,
                exc_tb.tb_lineno
            )
