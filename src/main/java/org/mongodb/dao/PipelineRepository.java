package org.mongodb.dao;

import org.mongodb.model.PipelineTemplate;
import org.springframework.data.mongodb.repository.MongoRepository;




public interface PipelineRepository extends MongoRepository<PipelineTemplate, String>, CustomPipelineRepository {

    public PipelineTemplate findByName(String name);

}
