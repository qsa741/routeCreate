import xml.etree.ElementTree as elementTree
import os


def makeXml(data):

    tree = elementTree.parse(os.path.join('Sample/' + data['sampleXml']))
    root = tree.getroot()

    for node in root:
        checkNode(tree, node, data['extract'])

    tree = elementTree.parse(os.path.join('Sample/Collect_Sample.xml'))
    root = tree.getroot()

    print("파일생성 : " + data['extract']['id'] + ".xml")

    for node in root:
        checkNode(tree, node, data['collect'])

    print("파일생성 : " + data['collect']['id'] + ".xml")

def checkNode(tree, nodes, data):
    if len(nodes) == 0:

        if nodes.text is None:
            nodes.text = ""

        if '?' in nodes.text:
            nodes.text = data[nodes.tag]

        tree.write(os.path.join('File/' + data['id'] + '.xml'),short_empty_elements=False)

    else:
        for node in nodes:
            checkNode(tree, node, data)




