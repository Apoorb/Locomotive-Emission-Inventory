import os
import xml.etree.ElementTree as ET
import copy

path_dir_templ = (r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
                  r"\HMP - TCEQ Projects - Documents"
                  r"\2020 Texas Statewide Locomotive and Rail Yard EI\Tasks"
                  r"\Task5_ Statewide_2020_AERR_EI\Ref\MOVESsccXMLformat\Output")
path_templ = os.path.join(path_dir_templ, "MOVESsccXMLformat_Test_xml.xml")

templ_tree = ET.parse(path_templ)

templ_root = templ_tree.getroot()

for templ_child in templ_root:
    print(templ_child.tag, templ_child.attrib)


for i in templ_root[0]:
    print(i.tag)

for i in templ_root[1][0][6]:
    print(i.tag)



ns = {'header': "http://www.exchangenetwork.net/schema/header/2",
      'payload': "http://www.exchangenetwork.net/schema/cer/1"
      }


for elem in templ_root[0].iter():
    print(elem.tag, elem.text)


print(ET.tostring(templ_root[0], encoding='utf8').decode('utf8'))

templ_root.findall('*/header:AuthorName', ns)

templ_root.findall('.//payload:UserIdentifier', ns)

templ_root.findall("*/header:AuthorName", ns)

templ_root.findall('header:Header', ns)
templ_root.findall('header:Payload/payload:CERS/payload:Location', ns)


templ_root.findall("Payload", ns)


location_root = templ_root.findall("header:Payload/payload:CERS", ns)[0]
all_counties = templ_root.findall(
    "./header:Payload/payload:CERS/payload:Location", ns)

test = copy.deepcopy(all_counties[0])
change_fips = test.find("payload:StateAndCountyFIPSCode", ns)
change_fips.text = "99999"
location_root.append(test)
all_counties = templ_root.findall(
    "./header:Payload/payload:CERS/payload:Location", ns)

i = 1
for elem in test.iter():
    print(elem, elem.text)
    i = i + 1
    if i > 3:
        break;


county_new = templ_root.findall("./header:Payload/payload:CERS/payload:Location"
                    "/[payload:StateAndCountyFIPSCode='99999']", ns)

location_root.remove(county_new[0])

all_counties = templ_root.findall(
    "./header:Payload/payload:CERS/payload:Location", ns)
print(ET.tostring(county_new[0], encoding='utf8').decode('utf8'))



i = 1
for elem in templ_root.iter():
    print(elem, elem.text)
    i = i + 1
    if i > 60:
        break;


for tags in templ_root:
    print(tags, tags.text)
    for tags_child in tags:
        print(tags_child, tags_child.text)


for statefips in templ_root.iter("{http://www.exchangenetwork.net/schema/cer/1}StateAndCountyFIPSCode"):
    print(statefips.text)
