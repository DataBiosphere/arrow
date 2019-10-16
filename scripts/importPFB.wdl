workflow import_PFB_workflow {
    call import_PFB
}

task import_PFB {
    String workspace_namespace
    String workspace_name
    String pfb_url
    String env

    runtime {
        docker: "gcr.io/terra-arrow-dev/arrow:crom"
        memory: "4G"
    }
    command <<<
        python3 /usr/src/app/avroToRawls.py '${workspace_namespace}' '${workspace_name}' '${pfb_url}' $(gcloud auth print-access-token) '${env}'
    >>>
    output {
        String out = read_string(stdout())
        String err = read_string(stderr())
    }
}
