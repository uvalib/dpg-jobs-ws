class PublishToApTrust < BaseJob
   def set_originator(message)
      @status.update_attributes( :originator_type=>"Metadata", :originator_id=>message[:metadata].id )
   end

   def do_workflow(message)
      raise "Parameter 'metadata' is required" if message[:metadata].blank?
      metadata = message[:metadata]

      if metadata.preservation_tier_id.blank? || !metadata.preservation_tier_id.blank? && metadata.preservation_tier_id < 2
         fatal_error("Preservation tier must be greater than 1")
      end

      # Generate bag, submit to aptrust and get resultant etag
      etag = PublishToApTrust.do_submission(metadata, logger)
      if etag.blank?
         fatal_error("Bag submission failed")
      end

      # poll APTrust to follow submission status. only when it is done end this job
      logger.info("Polling APTrust to monitor submission status")
      while (true) do
         sleep(1.minute)

         resp = ApTrust::status(etag)
         if !resp.nil?
            apt_status.update(status: resp[:status], note: resp[:note])
            logger.info("Status: #{resp[:status]}, stage: #{resp[:stage]}")

            if resp[:status] == "Failed" || resp[:status] == "Success"
               logger.info("APTrust submission #{resp[:status]}")
               apt_status.update(finished_at: resp[:finished_on], object_id: resp[:object_id])
               break
            end
         end
      end
   end

   def self.do_submission(metadata, logger = Logger.new(STDOUT) )
      if metadata.preservation_tier_id.blank? || metadata.preservation_tier_id == 1
         logger.error "Preservation tier [#{metadata.preservation_tier_id}] not suitable for submission to APTrust"
         return nil
      end
      apt_status = metadata.ap_trust_status
      if apt_status.present? && apt_status.refresh
         if apt_status.status == "Success"
            logger.info "Skipping Metadata #{metadata.id} has already been submitted to AP Trust."
            return apt_status.etag
         end
      end

      storage = "Standard"
      storage = "Glacier-VA" if metadata.preservation_tier_id == 2
      logger.info "Create new bag flagged for #{storage} storage"
      bag = Bagit::Bag.new({bag: "tracksys-#{metadata.type.downcase}-#{metadata.id}",
         title: metadata.title,
         pid: metadata.pid,
         storage: storage,
         collection: metadata.collection_name
         }, logger)

      logger.info "Adding masterfiles to bag..."
      master_file_cnt = 0
      metadata.master_files.each do |mf|
         logger.info "   #{mf.filename}"
         unit = mf.unit
         if unit.intended_use_id == 110
            mfp = File.join(Settings.archive_mount, unit.directory, mf.filename)
            bag.add_file( mf.filename, mfp)
            master_file_cnt += 1
         end
      end

      if master_file_cnt == 0
         logger.error("No master files qualify for APTrust (intended use 110)")
         return nil
      else
         logger.info "Added #{master_file_cnt} master files to bag"
      end

      logger.info "Add XML Metadata"
      bag.add_file("#{metadata.pid}.xml") { |io| io.write Hydra.desc(metadata) }

      logger.info "Generate manifests"
      bag.generate_manifests

      logger.info "Generate tar file"
      tarfile = bag.tar

      # create APTrust status record for this metadata...
      logger.info("Submitting bag to APTrust")
      apt_status = metadata.ap_trust_status
      if apt_status.nil?
         apt_status = ApTrustStatus.create(metadata: metadata, status: "Submitted")
      else
         apt_status.update(status:"Submitted", etag: nil, finished_at: nil, note: nil)
      end
      etag = ApTrust::submit( tarfile )
      apt_status.update(etag: etag)
      logger.info("Submitted; etag=#{etag}")

      logger.info("cleaning up bag working files")
      bag.cleanup

      return etag
   end
end 
