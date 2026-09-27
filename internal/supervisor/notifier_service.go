package supervisor

import (
	"github.com/yohn-jp/jinushi/internal/model"
)

// notifyRunChange wakes subscriptions only after their source-of-truth update
// has committed. The store remains responsible for cursors and gap detection.
func (s *Service) notifyRunChange(runID string) {
	if s.notifier != nil {
		s.notifier.notify(runID)
	}
}

func (s *Service) persistRunEvent(run model.Run, event *model.Event) error {
	if _, err := s.store.Update(run, event); err != nil {
		return err
	}
	if event != nil {
		s.notifyRunChange(run.ID)
	}
	return nil
}

func (s *Service) persistRunEvents(run model.Run, events []model.Event) error {
	if _, err := s.store.UpdateWithEvents(run, events); err != nil {
		return err
	}
	if len(events) > 0 {
		s.notifyRunChange(run.ID)
	}
	return nil
}

func (s *Service) persistEvent(runID string, event model.Event) error {
	if _, err := s.store.AppendEvent(runID, event); err != nil {
		return err
	}
	s.notifyRunChange(runID)
	return nil
}

func (s *Service) persistOutputChunk(runID, stream string, data []byte, maxRetained int64, event model.Event) (model.OutputStream, error) {
	meta, err := s.store.AppendOutputWithEvent(runID, stream, data, maxRetained, event)
	if err != nil {
		return model.OutputStream{}, err
	}
	s.notifyRunChange(runID)
	return meta, nil
}

func (s *Service) persistOutputGap(runID, stream string, observedBytes int64, event model.Event) (model.OutputStream, error) {
	meta, err := s.store.RecordOutputGapWithEvent(runID, stream, observedBytes, event)
	if err != nil {
		return model.OutputStream{}, err
	}
	s.notifyRunChange(runID)
	return meta, nil
}
