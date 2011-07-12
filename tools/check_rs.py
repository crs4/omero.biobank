"""
Check the rs annotation of a SNP versus NCBI dbSNP.

dbSNP data is read from fasta dumps downloaded from:
ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta
"""

# FIXME: not finished!

from bl.core.seq.io.fasta import RawFastaReader


class Reader(RawFastaReader):

  def next(self):
    header, seq = super(Reader, self).next()
    rs_id, pos = self.__parse_header(header)
    left_flank, right_flank = self.__parse_seq(seq, pos)
    return rs_id, left_flank, right_flank

  def __parse_header(self, header):
    header = header.split("|")
    rs_id = header[2].split(" ", 1)[0]
    pos = int(header[3].rsplit("=", 1)[-1])
    return rs_id, pos

  def __parse_seq(self, seq, pos):
    seq = seq.replace(" ", "")
    return seq[:pos-1], seq[pos:]


if __name__ == "__main__":
  fn = "rs_chMT.fas"
  f = open(fn)
  reader = Reader(f)
  print reader.next()
  f.close()
