class NZBBuilder:
  def __init__(self, title, poster, date, subject, password=None):
    self.title = title
    self.tags = set()
    self.password = password
    self.poster = poster
    self.date = date
    self.subject = subject
    self.groups = set()
    self.segments = []

  def add_tag(self, tag):
    self.tags.add(tag)

  def add_segment(self, group, message_id, size):
    self.groups.add(group)
    self.segment.append((message_id, size))

  def __str__(self):
    # title, (tags, ...), file(poster, date, subject, (group, ...), ((message_id, bytes), ...)
    nzb = ElementTree.Element('nzb', {'xmlns': "http://www.newzbin.com/DTD/2003/nzb"})
    head = ElementTree.SubElement(nzb, 'head')
    ElementTree.SubElement(head, 'meta', {'type': 'title'}).text = self.title
    for tag in self.tags:
      ElementTree.SubElement(head, 'meta', {'type': 'tag'}).text = tag
    if self.password:
      ElementTree.SubElement(head, 'meta', {'type': 'password'}).text = self.password
    file_entry = ElementTree.SubElement(nzb, 'file', {
        'poster': self.poster,
        'date': self.date,
        'subject': self.subject
    })
    groups = ElementTree.SubElement(file_entry, 'groups')
    for group in self.groups:
      ElementTree.SubElement(head, 'group').text = group
    segments = ElementTree.SubElement(file_entry, 'segments')
    for idx, segment in enumerate(self.segment, start=1):
      attrs = { 'bytes': segment[0], 'number': idx }
      ElementTree.SubElement(head, 'segment', attrs).text = segment[0]

