package client

func MultiRaft(group string) *Protocol {
	return &Protocol{
		MultiRaft: &MultiRaftProtocol{
			Group: group,
		},
	}
}

func MultiPrimary(group string) *Protocol {
	return &Protocol{
		MultiPrimary: &MultiPrimaryProtocol{
			Group: group,
		},
	}
}

func MultiLog(group string) *Protocol {
	return &Protocol{
		MultiLog: &MultiLogProtocol{
			Group: group,
		},
	}
}

type Protocol struct {
	MultiRaft *MultiRaftProtocol
	MultiPrimary *MultiPrimaryProtocol
	MultiLog *MultiLogProtocol
}

type MultiRaftProtocol struct {
	Group string
}

type MultiPrimaryProtocol struct {
    Group string
}

type MultiLogProtocol struct {
    Group string
}
