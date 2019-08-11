import sys
import xml.etree.ElementTree as ET


def read_file(filename):
    try:
        nodes = open(filename, 'r')
        text = nodes.readlines()
        content = [item.replace('\n', '') for item in text]
        return content
    except IOError:
        print("Couldn't open the file {}".format(filename))
        exit()


def write_xml_file(filename, declaration, dom):
    try:
        xml = open(filename, 'w')
        content = ET.tostring(dom, encoding='utf8', method='html').decode()

        xml.write(declaration)
        xml.write(content)
        xml.close()

    except IOError:
        print("Couldn't write the file {}".format(filename))
        exit()


def core_site(nodes):
    master = nodes[0]

    root = ET.Element('configuration')
    prop = ET.SubElement(root, 'property')
    name = ET.SubElement(prop, 'name')
    value = ET.SubElement(prop, 'value')

    name.text = 'fs.default.name'
    value.text = 'hdfs://{}:9000'.format(master)

    declaration = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n'
    write_xml_file('core-site.xml', declaration, root)


def hdfs_site():
    root = ET.Element('configuration')

    property_namenode = ET.SubElement(root, 'property')
    name_namenode = ET.SubElement(property_namenode, 'name')
    value_namenode = ET.SubElement(property_namenode, 'value')
    name_namenode.text = 'dfs.namenode.name.dir'
    value_namenode.text = '/home/hadoop/data/nameNode'

    property_datanode = ET.SubElement(root, 'property')
    name_datanode = ET.SubElement(property_datanode, 'nome')
    value_datanode = ET.SubElement(property_datanode, 'value')
    name_datanode.text = 'dfs.datanode.data.dir'
    value_datanode.text = '/home/hadoop/data/dataNode'

    property_replication = ET.SubElement(root, 'property')
    name_replication = ET.SubElement(property_replication, 'name')
    value_replication = ET.SubElement(property_replication, 'value')
    name_replication.text = 'dfs.replication'
    value_replication.text = '1'

    declaration = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n'
    write_xml_file('hdfs-site.xml', declaration, root)


def mapred_site():
    root = ET.Element('configuration')

    property_mapreduce_framework = ET.SubElement(root, 'property')
    name_mapreduce_framework = ET.SubElement(
        property_mapreduce_framework, 'name')
    value_mapreduce_framework = ET.SubElement(
        property_mapreduce_framework, 'value')
    name_mapreduce_framework.text = 'mapreduce.framework.name'
    value_mapreduce_framework.text = 'yarn'

    property_app_mapreduce = ET.SubElement(root, 'property')
    name_app_mapreduce = ET.SubElement(property_app_mapreduce, 'name')
    value_app_mapreduce = ET.SubElement(property_app_mapreduce, 'value')
    name_app_mapreduce.text = 'yarn.app.mapreduce.am.resource.mb'
    value_app_mapreduce.text = '512'

    property_mapreduce_map = ET.SubElement(root, 'property')
    name_mapreduce_map = ET.SubElement(property_mapreduce_map, 'name')
    value_mapreduce_map = ET.SubElement(property_mapreduce_map, 'value')
    name_mapreduce_map.text = 'mapreduce.map.memory.mb'
    value_mapreduce_map.text = '256'

    property_mapreduce_reduce = ET.SubElement(root, 'property')
    name_mapreduce_reduce = ET.SubElement(property_mapreduce_reduce, 'name')
    value_mapreduce_reduce = ET.SubElement(property_mapreduce_reduce, 'value')
    name_mapreduce_reduce.text = 'mapreduce.reduce.memory.mb'
    value_mapreduce_reduce.text = '256'

    declaration = '<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n'
    write_xml_file('mapred-site.xml', declaration, root)


def yarn_site(nodes):
    master = nodes[0]

    root = ET.Element('configuration')

    property_acl = ET.SubElement(root, 'property')
    name_acl = ET.SubElement(property_acl, 'name')
    value_acl = ET.SubElement(property_acl, 'value')
    name_acl.text = 'yarn.acl.enable'
    value_acl.text = '0'

    property_resourcemanager = ET.SubElement(root, 'property')
    name_resourcemanager = ET.SubElement(property_resourcemanager, 'name')
    value_resourcemanager = ET.SubElement(property_resourcemanager, 'value')
    name_resourcemanager.text = 'yarn.resourcemanager.hostname'
    value_resourcemanager.text = master

    property_nodemanager_aux = ET.SubElement(root, 'property')
    name_nodemanager_aux = ET.SubElement(property_nodemanager_aux, 'name')
    value_nodemanager_aux = ET.SubElement(property_nodemanager_aux, 'value')
    name_nodemanager_aux.text = 'yarn.nodemanager.aux-services'
    value_nodemanager_aux.text = 'mapreduce_shuffle'

    property_nodemanager = ET.SubElement(root, 'property')
    name_nodemanager = ET.SubElement(property_nodemanager, 'name')
    value_nodemanager = ET.SubElement(property_nodemanager, 'value')
    name_nodemanager.text = 'yarn.nodemanager.resource.memory-mb'
    value_nodemanager.text = '1536'

    property_scheduler_maximum = ET.SubElement(root, 'property')
    name_scheduler_maximum = ET.SubElement(property_scheduler_maximum, 'name')
    value_scheduler_maximum = ET.SubElement(
        property_scheduler_maximum, 'value')
    name_scheduler_maximum.text = 'yarn.scheduler.maximum-allocation-mb'
    value_scheduler_maximum.text = '1536'

    property_scheduler_minimum = ET.SubElement(root, 'property')
    name_scheduler_minimum = ET.SubElement(property_scheduler_minimum, 'name')
    value_scheduler_minimum = ET.SubElement(
        property_scheduler_minimum, 'value')
    name_scheduler_minimum.text = 'yarn.scheduler.maximum-allocation-mb'
    value_scheduler_minimum.text = '128'

    property_nodemanager_vmem_check = ET.SubElement(root, 'property')
    name_nodemanager_vmem_check = ET.SubElement(
        property_nodemanager_vmem_check, 'name')
    value_nodemanager_vmem_check = ET.SubElement(
        property_nodemanager_vmem_check, 'value')
    name_nodemanager_vmem_check.text = 'yarn.nodemanager.vmem-check-enabled'
    value_nodemanager_vmem_check.text = 'false'

    declaration = '<?xml version="1.0"?>\n'
    write_xml_file('yarn-site.xml', declaration, root)


def slaves(nodes):
    try:
        txt = open('slaves', 'w')

        string = '\n'.join(nodes) + '\n'
        txt.write(string)

        txt.close()
    except IOError:
        print("Couldn't write the file slaves")
        exit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Execution: python3 generate_hadoop_files.py hadoop_nodes.txt')
        exit()

    content = read_file(sys.argv[1])

    core_site(content)
    yarn_site(content)
    hdfs_site()
    mapred_site()
    slaves(content)
