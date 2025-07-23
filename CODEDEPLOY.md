
# Installing the CodeDeploy agent on EC2
```
#!/bin/bash
sudo yum update -y
sudo yum install -y ruby wget
wget https://aws-codedeploy-ap-south-1.s3.ap-south-1.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
sudo service codedeploy-agent status
```


# create a bucket and enable versioning
```
aws s3 mb s3://aws-devops-course-ravi-cd1
aws s3api put-bucket-versioning --bucket aws-devops-course-ravi-cd1 --versioning-configuration Status=Enabled 
```

# deploy the files into S3
```
aws deploy push --application-name my-deploy-app --s3-location s3://aws-devops-course-ravi-cd1/codedeploy-demo/app.zip --ignore-hidden-files 
```
1800 896 9999