bootstrap:
	flytectl demo start
	sh docker_build.sh -v 0.1
	docker push localhost:30000/workflows:0.1
	pyflyte register --image localhost:30000/pryon_workflows:0.1 workflows
	

register:
	pyflyte register --image localhost:30000/workflows:0.1 workflows
