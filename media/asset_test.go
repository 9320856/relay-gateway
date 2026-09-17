package media

import "testing"

func TestAssetCapabilityIsUnpredictableAndHashed(t *testing.T) {
	s := NewAssetService()
	a, capability, err := s.NewAsset("object")
	if err != nil {
		t.Fatal(err)
	}
	if a.PublicID == "" || capability == "" || a.PublicID == capability {
		t.Fatal("tokens missing or reused")
	}
	if a.CapabilityHash != HashCapability(capability) || !s.ValidateCapability(a, capability) {
		t.Fatal("capability validation failed")
	}
	if s.ValidateCapability(a, capability+"x") {
		t.Fatal("wrong capability accepted")
	}
	b, c2, _ := s.NewAsset("object")
	if a.PublicID == b.PublicID || capability == c2 {
		t.Fatal("tokens repeated")
	}
}
