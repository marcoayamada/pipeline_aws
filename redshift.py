import logging


def create_redshift_cluster(redshift_conn, identifier, node_type, cluster_type,
                            number_nodes, dbname, username, password, role_arn,
                            port=5439):
    try:
        print('Creating redshift cluster....')
        if cluster_type == 'single-node':
            response = redshift_conn.create_cluster(
                ClusterIdentifier=identifier,
                NodeType=node_type,
                ClusterType=cluster_type,
                DBName=dbname,
                Port=port,
                MasterUsername=username,
                MasterUserPassword=password,
                IamRoles=[role_arn]
            )
        else:
            response = redshift_conn.create_cluster(
                ClusterIdentifier=identifier,
                NodeType=node_type,
                ClusterType=cluster_type,
                NumberOfNodes=number_nodes,
                DBName=dbname,
                Port=port,
                MasterUsername=username,
                MasterUserPassword=password,
                IamRoles=[role_arn]
            )
        print('>> {}'.format(response))
        return response
    except Exception as e:
        logging.error(e)
        return False


def delete_redshift_cluster(redshift_conn, identifier, skip_snapshot=True):
    try:
        print('Deleting redshift cluster....')
        response = redshift_conn.delete_cluster(
            ClusterIdentifier=identifier,
            SkipFinalClusterSnapshot=skip_snapshot
        )
        print('>> {}'.format(response))
        return True
    except Exception as e:
        logging.error(e)
        return False


def set_external_access(ec2_conn, vpc_id, redshift_port):
    try:
        print('Opening port to external access to redshift cluster....')
        vpc = ec2_conn.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]

        default_sg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(redshift_port),
            ToPort=int(redshift_port)
        )
        print('>> {}'.format(default_sg))
        return True

    except Exception as e:
        print(e)
        return False
